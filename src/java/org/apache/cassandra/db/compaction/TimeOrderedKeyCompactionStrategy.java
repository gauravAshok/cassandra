/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.ComparablePair;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.filter;

/**
 * A compaction strategy more suited to SQ's write pattern Its goal is to store SQ msgs efficiently
 * by improving on space utilization and trading off on read performance.
 *
 * <p>Varadhi/Bifrost SQ pattern comprise of 1) bulk of timestamp'd msg inserts. eg: insert into SQ
 * (t1, msg1) insert into SQ (t2, msg2) .. so on.
 *
 * <p>2) bulk of msg deletes with the help of a secondary index. Using secondary index we can get
 * the partition key from msg_id alone. eg: select pk, msg_id, grp_id, * from index where msg_id =
 * "msg1" (or any other clause). delete (t2, msg2) from SQ delete (t1, msg1) from SQ.
 *
 * <p>TODO: expand
 */
public class TimeOrderedKeyCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeOrderedKeyCompactionStrategy.class);

    private final TimeOrderedKeyCompactionStrategyOptions options;
    private final TimeWindowCompactionStrategyOptions twcsOptions;
    private final Map<SSTableReader, SSTableStats> sstables = new HashMap<>();

    private volatile int estimatedRemainingTasks;

    public TimeOrderedKeyCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new TimeOrderedKeyCompactionStrategyOptions(options);
        this.twcsOptions = this.options.twcsOptions;

        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION)
                && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for TOKCS");
        }
        else
        {
            logger.debug("Enabling tombstone compactions for TOKCS");
        }
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        SSTablesForCompaction previousCandidate = null;
        while (true)
        {
            SSTablesForCompaction candidate = getNextBackgroundSSTables(gcBefore);

            if (candidate == SSTablesForCompaction.EMPTY)
            {
                return null;
            }

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy
            // manager
            if (candidate.equals(previousCandidate))
            {
                logger.warn(
                        "Could not acquire references for compacting SSTables {} which is not a problem per se,"
                                + "unless it happens frequently, in which case it must be reported. Will retry later.",
                        candidate);
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(candidate.sstables, OperationType.COMPACTION);
            if (modifier != null)
            {
                logger.info("compaction task: {}", candidate);
                return buildCompactionTask(candidate, modifier, gcBefore);
            }
            previousCandidate = candidate;
        }
    }

    private AbstractCompactionTask buildCompactionTask(SSTablesForCompaction sstables, LifecycleTransaction modifier, int gcBefore)
    {
        gcBefore = sstables.tombstoneMerge ? CompactionManager.NO_GC : gcBefore;
        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, options, sstables.splitSSTable, sstables.tombstoneMerge);
    }

    /**
     * Tombstone sstables can be of 3 categories:
     * [expired, nearExpiry, latest]. See the inline documentation.
     * <p>
     * There are 2 parameters that will affect the performance of the operations on tables with this compaction strategy.
     * 1. Time overlap between sstables. High overlap -> High read latency.
     * 2. Time range of a sstable. Large range -> large size. Likelihood of it overlapping with other sstables also increases when the range is large, which
     * will lead to larger cost in compaction. Compaction will be most efficient if there are fewer files to compact at a time.
     * <p>
     * This compaction strategy aims to reduce the max overlaps and the number of sstables.
     *
     * @param gcBefore
     * @return
     */
    synchronized SSTablesForCompaction getNextBackgroundSSTables(final int gcBefore)
    {
        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
        {
            return SSTablesForCompaction.EMPTY;
        }

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::containsKey)));
        updateEstimatedCompactionsByTasks(uncompacting);

        CategorizedSSTables categorized = categorizeSStables(uncompacting, gcBefore);

        // look for compaction candidates based on the garbage that we have accumulated
        Optional<SSTablesForCompaction> compactionCandidate = getCompactionCandidateBasedOnSize(categorized);
        if (compactionCandidate.isPresent())
        {
            return compactionCandidate.get();
        }

        // look for compaction candidates based on the overlappings. more sstables overlapping will result in bad read latency.
        compactionCandidate = getCompactionCandidateBasedOnOverlaps(categorized);
        if (compactionCandidate.isPresent())
        {
            return compactionCandidate.get();
        }

        return SSTablesForCompaction.EMPTY;
    }

    Optional<SSTablesForCompaction> getCompactionCandidateBasedOnSize(CategorizedSSTables categorizedSSTables)
    {
        double globalGarbage = getEstimatedGarbage(categorizedSSTables.stats);
        double globalSizeOnDisk = categorizedSSTables.stats.onDiskLength;

        // look for garbage cleanup.
        List<OverlappingSet> overlappingTombstones = getDistinctOverlappingSSTables(categorizedSSTables.expiredTombstones);

        List<DataCompactionCandidate> candidates = overlappingTombstones.stream()
                .map(ot -> DataCompactionCandidate.get(categorizedSSTables.data, ot))
                .collect(Collectors.toList());

        // if we are crossing the global threshold, just pick up the set with the most garbage, otherwise only choose the sets that are full of garbage.
        // This is done so that many localised deletes can result in freeing space.
        Stream<DataCompactionCandidate> candidateStream = isTooMuchGarbage(globalSizeOnDisk, globalGarbage, true)
                ? candidates.stream()
                : candidates.stream().filter(c -> isTooMuchGarbage(c.stats.onDiskLength, c.estimatedGarbage, false));
        Optional<DataCompactionCandidate> candidate = candidateStream.max(Comparator.comparingLong(c -> c.stats.onDiskLength));

        if (!candidate.isPresent())
        {
            return Optional.empty();
        }

        List<SSTableStats> maxGarbageData = candidate.get().data;
        OverlappingSet maxGarbageTombstones = candidate.get().tombstones;

        TimeWindow dataSSTableTimeWindow = TimeWindow.merge(maxGarbageData.stream().map(s -> s.timeWindow).collect(Collectors.toList()));

        // If there are too many files OR (data size is over the threshold and the data set is too wide),
        // we need a sure shot way of reducing them.
        if (maxGarbageTombstones.sstables.size() + maxGarbageData.size() > getMaxFileCountForCompaction() || dataSSTableTimeWindow.duration > 2 * getWindowSizeInSec())
        {
            if (maxGarbageTombstones.timeWindow.duration > 2 * getWindowSizeInSec() || maxGarbageTombstones.sstables.size() > 1)
            {
                return Optional.of(new SSTablesForCompaction(toSSTableReader(maxGarbageTombstones.sstables), true, true));
            }
            List<OverlappingSet> overlappingDataSets = getDistinctOverlappingSSTables(maxGarbageData);
            int windowSz = getWindowSizeInSec();

            // we have many overlapping data sets.
            // we would like to merge all those files that are overlapping and are in a single compaction window.
            return overlappingDataSets.stream()
                    // group them by compaction window
                    .collect(Collectors.groupingBy(s -> toWindow(s.timeWindow.ts, windowSz)))
                    .values().stream()
                    // flatten all the sstables present in all of the overlapping sets lying in this compaction window
                    .map(v -> v.stream()
                            .flatMap(s -> s.sstables.stream())
                            .collect(Collectors.toList()))
                    // get the window with the most sstables
                    .max(Comparator.comparingInt(List::size))
                    // limit the count of sstables to maxFileCountForCompaction
                    .map(l -> limit(getMaxFileCountForCompaction(), l))
                    // create a compaction task
                    .map(l -> new SSTablesForCompaction(toSSTableReader(l), false, true));
        }
        else
        {
            List<SSTableReader> all = new ArrayList<>(toSSTableReader(maxGarbageTombstones.sstables));
            all.addAll(toSSTableReader(maxGarbageData));
            return Optional.of(new SSTablesForCompaction(all, false, false));
        }
    }

    private Optional<SSTablesForCompaction> getCompactionCandidateBasedOnOverlaps(CategorizedSSTables categorizedSSTables)
    {
        int maxFileForCompaction = getMaxFileCountForCompaction();
        List<SSTableStats> mergeableLatestTombstones = getMaxOverlappingSSTables(categorizedSSTables.latestTombstones);
        // if latest tombstones are overlapping too much, merge them without splitting.
        if (mergeableLatestTombstones.size() > 1)
        {
            return Optional.of(new SSTablesForCompaction(toSSTableReader(limit(maxFileForCompaction, mergeableLatestTombstones)), true, false));
        }

        List<OverlappingSet> mergeableNearExpiryTombstones = getDistinctOverlappingSSTables(categorizedSSTables.nearExpiryTombstones);
        // If nearExpiry tombstones are overlapping too much or if they are wide, merge & split them.
        for (OverlappingSet os : mergeableNearExpiryTombstones)
        {
            if (os.maxOverlap > 1 || os.timeWindow.duration > 2 * getWindowSizeInSec())
            {
                return Optional.of(new SSTablesForCompaction(toSSTableReader(limit(maxFileForCompaction, os.sstables)), true, true));
            }
        }

        List<SSTableStats> mergeableTombstones = getMaxOverlappingSSTables(categorizedSSTables.expiredTombstones);
        List<SSTableStats> mergeableData = getMaxOverlappingSSTables(categorizedSSTables.data);

        boolean mergingTombstones = mergeableTombstones.size() > mergeableData.size();
        List<SSTableStats> mergeableSSTables = mergingTombstones ? mergeableTombstones : mergeableData;

        if (mergeableSSTables.size() > 1)
        {
            return Optional.of(new SSTablesForCompaction(toSSTableReader(limit(maxFileForCompaction, mergeableSSTables)), mergingTombstones, true));
        }

        return Optional.empty();
    }

    static double getEstimatedGarbage(TombstoneCounts tombstoneCounts, long totalDataPartitions, long totalDataRows, long totalDataSizeOnDisk)
    {
        double avgPartitionSize = (double) totalDataSizeOnDisk / totalDataPartitions;
        double avgRowSize = (double) totalDataSizeOnDisk / totalDataRows;

        long partitionTombstones = Math.min(tombstoneCounts.partitionTombstones, totalDataPartitions);
        long rowTombstones = Math.min(tombstoneCounts.rowTombstones, totalDataRows);

        // we are over eastimating the garbage. so assuming that range tombstones are for the remaining undeleted partitions.
        long rangeTombstones = Math.min(tombstoneCounts.rangeTombstones / 2, totalDataPartitions - partitionTombstones);
        return (partitionTombstones * avgPartitionSize) + (rowTombstones * avgRowSize) + (rangeTombstones * avgPartitionSize / 3.0);
    }

    static double getEstimatedGarbage(OverallStats os)
    {
        return getEstimatedGarbage(os.tombstoneCounts, os.keyCount, os.rowCount, os.onDiskLength);
    }

    private synchronized SSTableStats getOrCompute(SSTableReader sstable)
    {
        if (!sstables.containsKey(sstable))
        {
            SSTableStats stats = new SSTableStats(sstable);
            sstables.put(sstable, stats);
            return stats;
        }
        return sstables.get(sstable);
    }

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        getOrCompute(sstable);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    static TimeWindow getTimeWindow(SSTableReader ssTableReader, int windowSizeInSec)
    {
        StatsMetadata metadata = ssTableReader.getSSTableMetadata();
        return toWindow(metadata.minKey / 1000, metadata.maxKey / 1000, windowSizeInSec);
    }

    static TimeWindow toWindow(long tsStartInclusiveInSec, long tsEndExlusiveInSec, int windowSizeInSec)
    {
        long lowerWindowBound = toWindow(tsStartInclusiveInSec, windowSizeInSec);
        long upperWindowBound = toWindow(tsEndExlusiveInSec, windowSizeInSec) + (tsEndExlusiveInSec % windowSizeInSec == 0 ? 0 : windowSizeInSec);

        return new TimeWindow(lowerWindowBound, (int) (upperWindowBound - lowerWindowBound));
    }

    static long toWindow(long timeInSec, int windowSizeInSec)
    {
        return (timeInSec / windowSizeInSec) * windowSizeInSec;
    }

    private void updateEstimatedCompactionsByTasks(Iterable<SSTableReader> sstables)
    {
        int windowSize = getWindowSizeInSec();

        Set<Long> tWindowSet = new HashSet<>();
        Set<Long> dWindowSet = new HashSet<>();
        int tWindowOccupancy = 0, dWindowOccupancy = 0;

        for (SSTableReader sstable : sstables)
        {
            TimeWindow tw = getTimeWindow(sstable, windowSize);
            if (sstable.getSSTableLevel() == SSTable.DATA_SSTABLE_LVL)
            {
                dWindowOccupancy += collectOccupacyStat(dWindowSet, windowSize, tw);
            }
            else
            {
                tWindowOccupancy += collectOccupacyStat(tWindowSet, windowSize, tw);
            }
        }

        int n = tWindowOccupancy - (2 * tWindowSet.size()) + dWindowOccupancy - (2 * dWindowSet.size());
        logger.debug("pending tasks: {}, occupied_windows: {};{}, total_occupacy: {};{}", n, dWindowSet.size(), tWindowSet.size(), dWindowOccupancy, tWindowOccupancy);
        this.estimatedRemainingTasks = n;
    }

    private static int collectOccupacyStat(Set<Long> windowSet, int windowSize, TimeWindow tw)
    {
        long endTs = tw.getEndTs();
        for (long ts = tw.ts; ts <= endTs; ts += windowSize) windowSet.add(ts);
        return tw.getWindowLength(windowSize);
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables.keySet());
        if (Iterables.isEmpty(filteredSSTables))
        {
            return null;
        }
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
        {
            return null;
        }
        return Collections.singleton(new TimeOrderedKeyCompactionTask(cfs, txn, gcBefore, options, true, false));
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {

        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.debug(
                    "Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem",
                    sstables);
            return null;
        }

        boolean onlyTombstones = sstables.stream().allMatch(s -> s.getSSTableLevel() == SSTable.TOMBSTONE_SSTABLE_LVL);

        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, options, true, onlyTombstones).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return this.estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TimeOrderedKeyCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    public String toString()
    {
        // TODO: add important options here.
        return "TimeOrderedKeyCompactionStrategy";
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return false;
    }

    private int getWindowSizeInSec()
    {
        return (int) TimeUnit.SECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
    }

    static class SSTablesForCompaction
    {
        final List<SSTableReader> sstables;
        final boolean tombstoneMerge;
        final boolean splitSSTable;

        static final SSTablesForCompaction EMPTY = new SSTablesForCompaction(Collections.emptyList(), false, true);

        public SSTablesForCompaction(List<SSTableReader> sstables, boolean tombstoneMerge, boolean splitSSTable)
        {
            this.sstables = sstables;
            this.tombstoneMerge = tombstoneMerge;
            this.splitSSTable = splitSSTable;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SSTablesForCompaction that = (SSTablesForCompaction) o;
            return sstables.equals(that.sstables);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sstables);
        }

        @Override
        public String toString()
        {
            ArrayList<String> tombstoneGens = new ArrayList<>(sstables.size());
            ArrayList<String> dataGens = new ArrayList<>(sstables.size());
            for (SSTableReader sstable : sstables)
            {
                int gen = sstable.descriptor.generation;
                if (sstable.getSSTableLevel() == SSTable.TOMBSTONE_SSTABLE_LVL)
                {
                    tombstoneGens.add(String.valueOf(gen));
                }
                else
                {
                    dataGens.add(String.valueOf(gen));
                }
            }

            return "SSTablesForCompaction{" +
                    "\n tombstones=" + String.join(";", tombstoneGens) +
                    ",\n data=" + String.join(";", dataGens) +
                    ",\n tombstoneMerge=" + tombstoneMerge +
                    ",\n splitSSTable=" + splitSSTable +
                    "\n}";
        }
    }

    private boolean isTooMuchGarbage(double totalSizeOnDiskBytes, double garbageInBytes, boolean global)
    {
        double percentage = global ? options.windowCompactionGlobalSizePercent : options.windowCompactionSizePercent;
        double absolute = global ? options.windowCompactionGlobalSizeInMB : options.windowCompactionSizeInMB;
        double threshold = Double.min(totalSizeOnDiskBytes * (percentage / 100.0), absolute * FileUtils.ONE_MB);
        return garbageInBytes >= threshold;
    }

    private static class TombstoneCounts
    {
        long partitionTombstones = 0;
        long rowTombstones = 0;
        long rangeTombstones = 0;

        public void add(TombstoneCounts counts)
        {
            partitionTombstones += counts.partitionTombstones;
            rowTombstones += counts.rowTombstones;
            rangeTombstones += counts.rangeTombstones;
        }
    }

    static class CategorizedSSTables {
        public final List<SSTableStats> data;
        public final List<SSTableStats> expiredTombstones;
        public final List<SSTableStats> nearExpiryTombstones;
        public final List<SSTableStats> latestTombstones;
        public final OverallStats stats;

        public CategorizedSSTables(List<SSTableStats> data, List<SSTableStats> expiredTombstones, List<SSTableStats> nearExpiryTombstones, List<SSTableStats> latestTombstones, OverallStats stats)
        {
            this.data = data;
            this.expiredTombstones = expiredTombstones;
            this.nearExpiryTombstones = nearExpiryTombstones;
            this.latestTombstones = latestTombstones;
            this.stats = stats;
        }
    }

    CategorizedSSTables categorizeSStables(Iterable<SSTableReader> sstables, int gcBefore) {

        int gcGraceSeconds = cfs.metadata.params.gcGraceSeconds;
        long now = FBUtilities.nowInSeconds();
        long currentGcWindow = toWindow(now, gcGraceSeconds);

        // tombstone sstables that have expired and now can be compacted with data sstables to free up space.
        List<SSTableStats> compactableTombstones = new ArrayList<>(Iterables.size(sstables));
        // tombstones sstables that are only created recently and cannot be deleted. These will be meregd together to keep the overlap between sstables low.
        List<SSTableStats> latestTombstones = new ArrayList<>();
        // tombstone sstables that are about to be expired. They most likely will have 0 overlap. But if not (due to streaming), then merge it.
        List<SSTableStats> nearExpiryTombstones = new ArrayList<>();
        // data sstables
        List<SSTableStats> dataSSTables = new ArrayList<>();

        OverallStats overallStats = new OverallStats();
        sstables.forEach(s -> {
            SSTableStats stats = getOrCompute(s);
            collect(overallStats, stats);
            if (s.getSSTableLevel() == SSTable.TOMBSTONE_SSTABLE_LVL)
            {
                if (toWindow(s.getSSTableMetadata().maxLocalDeletionTime, gcGraceSeconds) >= currentGcWindow)
                {
                    latestTombstones.add(stats);
                }
                else if (s.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                {
                    compactableTombstones.add(stats);
                }
                else
                {
                    nearExpiryTombstones.add(stats);
                }
            }
            else
            {
                dataSSTables.add(stats);
            }
        });

        return new CategorizedSSTables(dataSSTables, compactableTombstones, nearExpiryTombstones, latestTombstones, overallStats);
    }

    static class SSTableStats
    {
        final SSTableReader sstable;
        final ICardinality cardinality;
        final long keyCount;
        final long rowCount;
        final TombstoneCounts tombstoneCounts;
        final TimeWindow timeWindow;

        public SSTableStats(SSTableReader sstable)
        {
            StatsMetadata meta = sstable.getSSTableMetadata();
            this.sstable = sstable;
            this.tombstoneCounts = new TombstoneCounts();
            this.tombstoneCounts.partitionTombstones += meta.partitionTombstones;
            this.tombstoneCounts.rowTombstones += meta.rowTombstones;
            this.tombstoneCounts.rangeTombstones += meta.rangeTombstones;

            ICardinality cardinality = null;
            try
            {
                cardinality = SSTableReader.getCardinality(sstable);
            }
            catch (IOException e)
            {
            }
            this.cardinality = cardinality;

            if (this.cardinality != null)
            {
                this.keyCount = cardinality.cardinality();
            }
            else
            {
                this.keyCount = sstable.estimatedKeys();
            }
            this.rowCount = sstable.getTotalRows();
            this.timeWindow = new TimeWindow(meta.minKey, (int) (meta.maxKey - meta.minKey));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SSTableStats that = (SSTableStats) o;
            return sstable.equals(that.sstable);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sstable);
        }

        @Override
        public String toString()
        {
            return "SSTableStats{" +
                    "sstable=" + sstable +
                    ", cardinality=" + cardinality +
                    ", keyCount=" + keyCount +
                    ", rowCount=" + rowCount +
                    ", tombstoneCounts=" + tombstoneCounts +
                    ", timeWindow=" + timeWindow +
                    '}';
        }
    }

    static class OverallStats
    {
        long keyCount = 0;
        long rowCount = 0;
        TombstoneCounts tombstoneCounts = new TombstoneCounts();
        long onDiskLength = 0;

        @Override
        public String toString()
        {
            return "OverallStats{" +
                    "keyCount=" + keyCount +
                    ", rowCount=" + rowCount +
                    ", tombstoneCounts=" + tombstoneCounts +
                    ", onDiskLength=" + onDiskLength +
                    '}';
        }
    }

    private static void collect(OverallStats os, SSTableStats ss)
    {
        if (ss.sstable.getSSTableLevel() == SSTable.DATA_SSTABLE_LVL)
        {
            os.keyCount += ss.keyCount;
            os.rowCount += ss.rowCount;
            os.onDiskLength += ss.sstable.onDiskLength();
        }
        else
        {
            os.tombstoneCounts.add(ss.tombstoneCounts);
        }
    }

    private static class TimeBoundary extends ComparablePair<Long, Integer>
    {
        final int idx;

        TimeBoundary(int idx, Long time, boolean start)
        {
            super(time, start ? 1 : 0);
            this.idx = idx;
        }

        boolean isStartBoundary()
        {
            return this.right == 1;
        }
    }

    static class OverlappingSet
    {
        List<SSTableStats> sstables = new ArrayList<>();
        TimeWindow timeWindow;
        int maxOverlap;
    }

    private static class DataCompactionCandidate
    {
        OverlappingSet tombstones;
        List<SSTableStats> data;
        OverallStats stats;
        double estimatedGarbage;

        public DataCompactionCandidate(OverlappingSet tombstones, List<SSTableStats> data, OverallStats stats, double estimatedGarbage)
        {
            this.tombstones = tombstones;
            this.data = data;
            this.stats = stats;
            this.estimatedGarbage = estimatedGarbage;
        }

        public static DataCompactionCandidate get(List<SSTableStats> allData, OverlappingSet tombstones)
        {
            List<SSTableStats> overlappingData = getOverlappingSSTables(tombstones.timeWindow, allData);
            OverallStats stats = new OverallStats();
            for (SSTableStats ss : tombstones.sstables) collect(stats, ss);
            for (SSTableStats ss : overlappingData) collect(stats, ss);
            double garbage = getEstimatedGarbage(stats);
            return new DataCompactionCandidate(tombstones, overlappingData, stats, garbage);
        }
    }

    /**
     * Returns a list of set that overlaps with itself. The elements in the set are in the order of ts.
     *
     * @param sstables
     * @return
     */
    static List<OverlappingSet> getDistinctOverlappingSSTables(List<SSTableStats> sstables)
    {
        // TODO: confirm that elements in the overlapping set are ordered by ts.
        if (sstables.isEmpty())
        {
            return Collections.emptyList();
        }
        List<TimeBoundary> timeboundaries = new ArrayList<>();
        for (int i = 0; i < sstables.size(); ++i)
        {
            SSTableStats s = sstables.get(i);
            timeboundaries.add(new TimeBoundary(i, s.timeWindow.ts, true));
            timeboundaries.add(new TimeBoundary(i, s.timeWindow.getEndTs(), false));
        }
        Collections.sort(timeboundaries);

        List<OverlappingSet> sets = new ArrayList<>();
        OverlappingSet currentSet = new OverlappingSet();

        int currOverlap = -1;
        currentSet.maxOverlap = -1;
        long start = Long.MAX_VALUE, end = Long.MIN_VALUE;

        for (TimeBoundary tb : timeboundaries)
        {
            if (tb.isStartBoundary())
            {
                ++currOverlap;
                start = Long.min(start, tb.left);
                currentSet.sstables.add(sstables.get(tb.idx));

                if (currOverlap > currentSet.maxOverlap)
                {
                    currentSet.maxOverlap = currOverlap;
                }
            }
            else
            {
                --currOverlap;
                end = Long.max(end, tb.left);
                if (currOverlap == -1)
                {
                    currentSet.timeWindow = new TimeWindow(start, (int) (end - start));
                    sets.add(currentSet);

                    // reset
                    currentSet = new OverlappingSet();
                    start = Long.MAX_VALUE;
                    end = Long.MIN_VALUE;
                    currentSet.maxOverlap = -1;
                }
            }
        }
        return sets;
    }

    static List<SSTableStats> getMaxOverlappingSSTables(List<SSTableStats> sstables)
    {
        if (sstables.isEmpty())
        {
            return Collections.emptyList();
        }

        List<TimeBoundary> timeboundaries = new ArrayList<>();
        for (int i = 0; i < sstables.size(); ++i)
        {
            SSTableStats s = sstables.get(i);
            timeboundaries.add(new TimeBoundary(i, s.timeWindow.ts, true));
            timeboundaries.add(new TimeBoundary(i, s.timeWindow.getEndTs(), false));
        }
        Collections.sort(timeboundaries);

        List<Integer> maxOverlappingSet = new ArrayList<>();
        int maxOverlap = -1, currOverlap = -1;
        Set<Integer> currOverlappingSet = new HashSet<>();

        for (TimeBoundary tb : timeboundaries)
        {
            if (tb.isStartBoundary())
            {
                ++currOverlap;
                currOverlappingSet.add(tb.idx);
                if (currOverlap > maxOverlap)
                {
                    maxOverlap = currOverlap;
                    maxOverlappingSet.clear();
                    maxOverlappingSet.addAll(currOverlappingSet);
                }
            }
            else
            {
                --currOverlap;
                currOverlappingSet.remove(tb.idx);
            }
        }

        return maxOverlappingSet.stream().map(sstables::get).collect(Collectors.toList());
    }

    static List<SSTableStats> getOverlappingSSTables(TimeWindow timeWindow, List<SSTableStats> sstables)
    {
        return sstables.stream().filter(s -> s.timeWindow.intersects(timeWindow)).collect(Collectors.toList());
    }

    static List<SSTableReader> toSSTableReader(List<SSTableStats>... stats)
    {
        ArrayList<SSTableReader> list = new ArrayList<>();
        for (List<SSTableStats> ssList : stats)
        {
            for (SSTableStats ss : ssList)
            {
                list.add(ss.sstable);
            }
        }
        return list;
    }

    private int getMaxFileCountForCompaction()
    {
        return cfs.getMaximumCompactionThreshold();
    }

    static <T> List<T> limit(int size, List<T> list)
    {
        return list.stream().limit(size).collect(Collectors.toList());
    }
}
