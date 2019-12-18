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
import org.apache.cassandra.utils.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
    private final String tableName;

    public TimeOrderedKeyCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.tableName = cfs.getTableName();
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
                logger.debug("{}: {}", tableName, candidate);
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
     * There are 2 parameters that will affect the performance of the operations on tables with tlatencyhis compaction strategy.
     * 1. Time overlap between sstables. Many files overlaping with each other -> High read .
     * 2. Time range of a sstable. Large range -> large size. Likelihood of it overlapping with other sstables also increases when the range is large, which
     * will lead to larger cost in compaction. Compaction will be most efficient if there are fewer files to compact at a time.
     * <p>
     * This compaction strategy aims to reduce the max overlap and the number of sstables.
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

        CategorizedSSTables categorized = categorizeSStables(uncompacting, gcBefore);

        if (logger.isDebugEnabled())
        {
            logger.debug("{}: {}", tableName, categorized.toString());
        }

        return firstOf(categorized::getCompactionCandidateBasedOnSize, categorized::getCompactionCandidateBasedOnOverlaps)
                .orElse(SSTablesForCompaction.EMPTY);
    }

    private void updateRemainingTasks(CategorizedSSTables categorizedSSTables, List<DataCompactionCandidate> candidatesBasedOnSize)
    {
        CategorizedSSTables sstablesForTaskEstimation = new CategorizedSSTables(
                new HashSet<>(categorizedSSTables.data),
                new HashSet<>(categorizedSSTables.expiredTombstones),
                new HashSet<>(categorizedSSTables.nearExpiryTombstones),
                new HashSet<>(categorizedSSTables.latestTombstones), null);

        int tasksBasedOnSize = candidatesBasedOnSize.stream().mapToInt(DataCompactionCandidate::estimateRemainingTasks).sum();
        // since actual data compaction will actually take care of reducing the overlaps and wideness
        // lets remove these sstables from the known sstables.
        // we can then then use the remaining sstables for estimating tasks just based on overlaps.
        for (DataCompactionCandidate c : candidatesBasedOnSize)
        {
            sstablesForTaskEstimation.data.removeAll(c.data);
            sstablesForTaskEstimation.expiredTombstones.removeAll(c.tombstones.sstables);
        }

        int tasksBasedOnOverlaps = sstablesForTaskEstimation.estimateRemainingTasksBasedOnOverlaps();
        estimatedRemainingTasks = tasksBasedOnSize + tasksBasedOnOverlaps;
        logger.debug("pending tasks: {}, basedOnSize: {}, basedOnOverlaps: {}", estimatedRemainingTasks, tasksBasedOnSize, tasksBasedOnOverlaps);
    }

    static double getEstimatedGarbage(TombstoneCounts tombstoneCounts, long totalDataPartitions, long totalDataRows, long totalDataSizeOnDisk)
    {
        double avgPartitionSize = totalDataPartitions == 0 ? 0 : (double) totalDataSizeOnDisk / totalDataPartitions;
        double avgRowSize = totalDataRows == 0 ? 0 : (double) totalDataSizeOnDisk / totalDataRows;

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

    public static TimeWindow getTimeWindowMs(SSTableReader ssTableReader, long windowSizeInMs)
    {
        StatsMetadata metadata = ssTableReader.getSSTableMetadata();
        return toWindowMs(metadata.minKey, metadata.maxKey, windowSizeInMs);
    }

    static TimeWindow toWindowMs(long tsStartInclusiveInMs, long tsEndExlusiveInMs, long windowSizeInMs)
    {
        long lowerWindowBound = toWindowMs(tsStartInclusiveInMs, windowSizeInMs);
        long upperWindowBound = toWindowMs(tsEndExlusiveInMs, windowSizeInMs) + (tsEndExlusiveInMs % windowSizeInMs == 0 ? 0 : windowSizeInMs);

        return new TimeWindow(lowerWindowBound, upperWindowBound - lowerWindowBound);
    }

    static long toWindowMs(long timeInMs, long windowSizeMs)
    {
        return (timeInMs / windowSizeMs) * windowSizeMs;
    }

    private static long collectOccupacyStat(Set<Long> windowSet, long windowSizeMs, TimeWindow tw)
    {
        long endTs = tw.getEndTs();
        for (long ts = tw.ts; ts < endTs; ts += windowSizeMs) windowSet.add(ts);
        return tw.getWindowLength(windowSizeMs);
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
        return String.format("TimeOrderedKeyCompactionStrategy{window:%d sec, split:%d, local:%dMB/%f%%, global:%dMB/%f%%}",
                getWindowSizeInMs() / 1000, options.splitFactor,
                options.windowCompactionSizeInMB, options.windowCompactionSizePercent,
                options.windowCompactionGlobalSizeInMB, options.windowCompactionGlobalSizePercent);
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return false;
    }

    private long getWindowSizeInMs()
    {
        return TimeUnit.MILLISECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
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

        public static SSTablesForCompaction create(List<SSTableStats> sstables, boolean tombstoneMerge, boolean splitSSTable)
        {
            return new SSTablesForCompaction(toSSTableReader(sstables), tombstoneMerge, splitSSTable);
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
                    "tombstoneMerge=" + tombstoneMerge +
                    ", splitSSTable=" + splitSSTable +
                    ", tombstones=[" + String.join(";", tombstoneGens) + "]" +
                    ", data=[" + String.join(";", dataGens) + "]" +
                    "}";
        }
    }

    private boolean isTooMuchGarbage(double totalSizeOnDiskBytes, double garbageBytes, boolean global)
    {
        double percentage = global ? options.windowCompactionGlobalSizePercent : options.windowCompactionSizePercent;
        double absolute = global ? options.windowCompactionGlobalSizeInMB : options.windowCompactionSizeInMB;
        double threshold = Double.min(totalSizeOnDiskBytes * (percentage / 100.0), absolute * FileUtils.ONE_MB);
        return garbageBytes >= threshold;
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

        @Override
        public String toString()
        {
            return "TombstoneCounts{" +
                    "p=" + partitionTombstones +
                    ", r=" + rowTombstones +
                    ", rng=" + rangeTombstones +
                    '}';
        }
    }

    static class SSTableStats
    {
        final SSTableReader sstable;
        final ICardinality cardinality;
        final long keyCount;
        final long rowCount;
        final TombstoneCounts tombstoneCounts;
        final TimeWindow timeWindowMs;

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
            this.timeWindowMs = new TimeWindow(meta.minKey, (int) (meta.maxKey - meta.minKey));
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
                    ", timeRange=" + timeWindowMs + " ms" +
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

    private static class TimeBoundary<T> extends ComparablePair<Long, Integer>
    {
        final T context;

        TimeBoundary(T context, Long time, boolean start)
        {
            super(time, start ? 1 : 0);
            this.context = context;
        }

        boolean isStartBoundary()
        {
            return this.right == 1;
        }
    }

    static class OverlappingSet
    {
        List<SSTableStats> sstables = new ArrayList<>();
        TimeWindow timeWindowMs;
        int maxOverlap;

        public OverlappingSet()
        {
        }

        public OverlappingSet(List<SSTableStats> sstables, TimeWindow timeWindowMs, int maxOverlap)
        {
            this.sstables = sstables;
            this.timeWindowMs = timeWindowMs;
            this.maxOverlap = maxOverlap;
        }

        @Override
        public String toString()
        {
            return "OverlappingSet{" +
                    "timeWindow=" + timeWindowMs + " ms" +
                    ", maxOverlap=" + maxOverlap +
                    ", sstables=" + idWithTimeRange(sstables) +
                    '}';
        }
    }

    class CategorizedSSTables
    {
        public final Collection<SSTableStats> data;
        public final Collection<SSTableStats> expiredTombstones;
        public final Collection<SSTableStats> nearExpiryTombstones;
        public final Collection<SSTableStats> latestTombstones;
        public final OverallStats stats;

        public CategorizedSSTables(Collection<SSTableStats> data, Collection<SSTableStats> expiredTombstones, Collection<SSTableStats> nearExpiryTombstones, Collection<SSTableStats> latestTombstones, OverallStats stats)
        {
            this.data = data;
            this.expiredTombstones = expiredTombstones;
            this.nearExpiryTombstones = nearExpiryTombstones;
            this.latestTombstones = latestTombstones;
            this.stats = stats;
        }

        @Override
        public String toString()
        {
            return "CategorizedSSTables{" +
                    "data=" + idString(data) +
                    ", expiredTombstones=" + idString(expiredTombstones) +
                    ", nearExpiryTombstones=" + idString(nearExpiryTombstones) +
                    ", latestTombstones=" + idString(latestTombstones) +
                    ", stats=" + stats +
                    '}';
        }

        public Optional<SSTablesForCompaction> getCompactionCandidateBasedOnSize()
        {
            double globalGarbage = getEstimatedGarbage(stats);
            double globalSizeOnDisk = stats.onDiskLength;
            logger.debug("{}: garbage: {}/{}", tableName, globalGarbage, globalSizeOnDisk);

            // look for garbage cleanup.
            List<OverlappingSet> overlappingTombstones = getDistinctOverlappingSSTables(expiredTombstones);

            Stream<DataCompactionCandidate> candidateStream = overlappingTombstones.stream()
                    .map(ot -> buildDataCompactionCandidate(data, ot));

            // if we are crossing the global threshold, just pick up the set with the most garbage, otherwise only choose the sets that are full of garbage.
            // This is done so that many localized deletes can result in compaction.
            candidateStream = isTooMuchGarbage(globalSizeOnDisk, globalGarbage, true)
                    ? candidateStream
                    : candidateStream.filter(c -> isTooMuchGarbage(c.stats.onDiskLength, c.estimatedGarbage, false));
            List<DataCompactionCandidate> candidates = candidateStream.collect(Collectors.toList());
            updateRemainingTasks(this, candidates);
            Optional<DataCompactionCandidate> candidate = candidates.stream().max(Comparator.comparingDouble(c -> c.estimatedGarbage));

            if (!candidate.isPresent())
            {
                logger.debug("{}: no significant garbage found", tableName);
                return Optional.empty();
            }

            return candidate.get().getSStablesForCompaction();
        }

        public Optional<SSTablesForCompaction> getCompactionCandidateBasedOnOverlaps()
        {
            int maxFileForCompaction = getMaxFileCountForCompaction();
            OverlappingSet mergeableLatestTombstones = getMaxOverlappingSSTables(latestTombstones);
            // if latest tombstones are overlapping too much, merge them without splitting.
            if (canMergeLatestTombstones(mergeableLatestTombstones))
            {
                logger.debug("{}: merging latest tombstone files", tableName);
                return Optional.of(SSTablesForCompaction.create(limit(maxFileForCompaction, mergeableLatestTombstones.sstables), true, false));
            }

            List<OverlappingSet> mergeableNearExpiryTombstones = getDistinctOverlappingSSTables(nearExpiryTombstones);
            // If nearExpiry tombstones are overlapping too much or if they are wide, merge & split them.
            for (OverlappingSet os : mergeableNearExpiryTombstones)
            {
                if (canMergeNearExpiryTombstones(os))
                {
                    logger.debug("{}: merging near expiry tombstone files", tableName);
                    return Optional.of(SSTablesForCompaction.create(limit(maxFileForCompaction, os.sstables), true, true));
                }
            }

            OverlappingSet mergeableTombstones = getMaxOverlappingSSTables(expiredTombstones);
            OverlappingSet mergeableData = getMaxOverlappingSSTables(data);

            boolean mergingTombstones = mergeableTombstones.sstables.size() > mergeableData.sstables.size();
            OverlappingSet mergeableSSTables = mergingTombstones ? mergeableTombstones : mergeableData;

            if ((mergingTombstones && canMergeExpiredTombstones(mergeableSSTables)) || (!mergingTombstones && canMergeData(mergeableSSTables)))
            {
                logger.debug("{}: merging {} files", tableName, mergingTombstones ? "expired tombstone" : "data");
                return Optional.of(SSTablesForCompaction.create(limit(maxFileForCompaction, mergeableSSTables.sstables), mergingTombstones, true));
            }

            return Optional.empty();
        }

        private boolean canMergeData(OverlappingSet mergeableSSTables)
        {
            return mergeableSSTables.maxOverlap > 1;
        }

        private boolean canMergeExpiredTombstones(OverlappingSet mergeableSSTables)
        {
            return mergeableSSTables.sstables.size() > 1;
        }

        private boolean canMergeNearExpiryTombstones(OverlappingSet expired)
        {
            return expired.maxOverlap > 1 || expired.timeWindowMs.duration > 2 * getWindowSizeInMs();
        }

        private boolean canMergeLatestTombstones(OverlappingSet mergeableLatestTombstones)
        {
            return mergeableLatestTombstones.maxOverlap > 1;
        }

        public int estimateRemainingTasksBasedOnOverlaps()
        {
            List<Collection<SSTableStats>> all = Arrays.asList(data, expiredTombstones, nearExpiryTombstones, latestTombstones);
            List<Predicate<OverlappingSet>> filters = Arrays.asList(this::canMergeData, this::canMergeExpiredTombstones, this::canMergeNearExpiryTombstones, this::canMergeLatestTombstones);

            assert all.size() == filters.size();

            return IntStream
                    .range(0, all.size())
                    .map(i ->
                            (int) getDistinctOverlappingSSTables(all.get(i))
                                    .stream()
                                    .filter(filters.get(i))
                                    .count()
                    )
                    .sum();
        }
    }

    CategorizedSSTables categorizeSStables(Iterable<SSTableReader> sstables, int gcBefore)
    {
        int gcGraceSecondsMs = cfs.metadata.params.gcGraceSeconds * 1000;
        long nowMs = System.currentTimeMillis();

        // tombstone sstables that have expired and now can be compacted with data sstables to free up space.
        List<SSTableStats> expiredTombstones = new ArrayList<>(Iterables.size(sstables));
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
                // If gcGraceSeconds > 0, i.e. tombstones will linger in sstables for some time and if they are created recently,
                // mark them as latest so that they are not compacted against data sstables just yet.

                if (s.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                {
                    expiredTombstones.add(stats);
                }
                else
                {
                    if (gcGraceSecondsMs > 0 &&
                            toWindowMs(s.getSSTableMetadata().maxLocalDeletionTime * 1000L, gcGraceSecondsMs) >= toWindowMs(nowMs, gcGraceSecondsMs))
                    {
                        latestTombstones.add(stats);
                    }
                    else
                    {
                        nearExpiryTombstones.add(stats);
                    }
                }
            }
            else
            {
                dataSSTables.add(stats);
            }
        });

        return new CategorizedSSTables(dataSSTables, expiredTombstones, nearExpiryTombstones, latestTombstones, overallStats);
    }

    public DataCompactionCandidate buildDataCompactionCandidate(Collection<SSTableStats> allData, OverlappingSet tombstones)
    {
        List<SSTableStats> overlappingData = getOverlappingSSTables(tombstones.timeWindowMs, allData);
        OverallStats stats = new OverallStats();
        for (SSTableStats ss : tombstones.sstables) collect(stats, ss);
        for (SSTableStats ss : overlappingData) collect(stats, ss);
        double garbage = getEstimatedGarbage(stats);
        return new DataCompactionCandidate(tombstones, overlappingData, stats, garbage);
    }

    private class DataCompactionCandidate
    {
        final OverlappingSet tombstones;
        final List<SSTableStats> data;
        final OverallStats stats;
        final double estimatedGarbage;
        final TimeWindow dataSSTableTimeWindowMs;
        final long dataSizeOnDisk;

        public DataCompactionCandidate(OverlappingSet tombstones, List<SSTableStats> data, OverallStats stats, double estimatedGarbage)
        {
            this.tombstones = tombstones;
            this.data = data;
            this.stats = stats;
            this.estimatedGarbage = estimatedGarbage;
            this.dataSSTableTimeWindowMs = TimeWindow.merge(data.stream().map(s -> s.timeWindowMs).collect(Collectors.toList()));
            this.dataSizeOnDisk = data.stream().mapToLong(s -> s.sstable.onDiskLength()).sum();
        }

        Optional<SSTablesForCompaction> getSStablesForCompaction()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("{}: candidate: {}", tableName, this);
            }

            // If there are too many files OR (data size is over the threshold and the data set is too wide),
            // we need a sure shot way of reducing them.
            if (!isCompactable())
            {
                if (needsTombstoneMerging())
                {
                    logger.debug("{}: too many / too wide tombstone files", tableName);
                    return Optional.of(SSTablesForCompaction.create(tombstones.sstables, true, true));
                }

                List<OverlappingSet> overlappingDataSets = getDistinctOverlappingSSTables(data);
                long windowSzInMs = getWindowSizeInMs();

                logger.debug("{}: too many data files", tableName);
                // we are here, which means that tombstone.timerange <= 2 * window && tombstone.size() <= 1.
                // data files could be too wide or many small files.
                // we would like to merge all those files that are overlapping or are in a single compaction window.
                return overlappingDataSets.stream()
                        // group them by compaction window
                        .collect(Collectors.groupingBy(s -> toWindowMs(s.timeWindowMs.ts, windowSzInMs)))
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
                        .map(l -> SSTablesForCompaction.create(l, false, true));
            }
            else
            {
                List<SSTableStats> all = Stream.concat(tombstones.sstables.stream(), data.stream()).collect(Collectors.toList());
                return Optional.of(SSTablesForCompaction.create(all, false, true));
            }
        }

        private boolean needsTombstoneMerging()
        {
            return tombstones.timeWindowMs.duration > 2 * getWindowSizeInMs() || tombstones.sstables.size() > 1;
        }

        private boolean isCompactable()
        {
            boolean tooManyFiles = tombstones.sstables.size() + data.size() > getMaxFileCountForCompaction();
            boolean tooMuchData = dataSSTableTimeWindowMs.duration > 2 * getWindowSizeInMs() && dataSizeOnDisk > options.compactionMaxSizeMB * FileUtils.ONE_MB;
            return !tooManyFiles && !tooMuchData;
        }

        public int estimateRemainingTasks()
        {
            if (isCompactable())
            {
                // 1 compaction between all tombstones & data
                return 1;
            }
            else
            {
                // 1 for merging / splitting tombstones, 1 for merging / splitting data, atleast 1 for eventual merging between tombstones & data.
                return needsTombstoneMerging() ? 3 : 2;
            }
        }

        @Override
        public String toString()
        {
            return "DataCompactionCandidate{" +
                    "tombstones=" + tombstones +
                    ", data=" + idWithTimeRange(data) +
                    ", stats=" + stats +
                    ", estimatedGarbage=" + estimatedGarbage +
                    ", dataSSTableTimeWindowMs=" + dataSSTableTimeWindowMs +
                    ", dataSizeOnDisk=" + dataSizeOnDisk +
                    '}';
        }
    }

    private static class DistinctOverlappingSSTablesState implements Consumer<TimeBoundary<SSTableStats>>
    {
        private final List<OverlappingSet> sets = new ArrayList<>();
        private OverlappingSet currentSet = new OverlappingSet();
        private int currOverlap = -1;
        private int maxOverlap = -1;
        private long start = Long.MAX_VALUE;
        private long end = Long.MIN_VALUE;

        public void accept(TimeBoundary<SSTableStats> tb)
        {
            if (tb.isStartBoundary())
            {
                ++currOverlap;
                start = Long.min(start, tb.left);
                currentSet.sstables.add(tb.context);

                if (currOverlap > maxOverlap)
                {
                    maxOverlap = currOverlap;
                }
            }
            else
            {
                --currOverlap;
                end = Long.max(end, tb.left);
                if (currOverlap == -1)
                {
                    currentSet.timeWindowMs = new TimeWindow(start, (int) (end - start));
                    currentSet.maxOverlap = maxOverlap;
                    sets.add(currentSet);

                    // reset
                    currentSet = new OverlappingSet();
                    maxOverlap = -1;
                    start = Long.MAX_VALUE;
                    end = Long.MIN_VALUE;
                }
            }
        }
    }

    static <T extends Consumer<TimeBoundary<SSTableStats>>, R> R reduceOverTimeBoundaries(Collection<SSTableStats> sstables, T state, Function<T, R> finisher)
    {
        sstables
                .stream()
                .map(s -> Stream.of(
                        new TimeBoundary<>(s, s.timeWindowMs.ts, true),
                        new TimeBoundary<>(s, s.timeWindowMs.getEndTs(), false)
                ))
                .flatMap(Function.identity())
                .sorted()
                .forEachOrdered(state);

        return finisher.apply(state);
    }

    /**
     * Returns a list of set that overlaps with itself. The elements in the set are in the order of ts.
     *
     * @param sstables
     * @return
     */
    static List<OverlappingSet> getDistinctOverlappingSSTables(Collection<SSTableStats> sstables)
    {
        return reduceOverTimeBoundaries(sstables, new DistinctOverlappingSSTablesState(), state -> state.sets);
    }

    private static class MaxOverlappingSSTablesState implements Consumer<TimeBoundary<SSTableStats>>
    {
        private final List<SSTableStats> set = new ArrayList<>();
        private final Set<SSTableStats> currentSet = new HashSet<>();
        private int maxOverlap = -1;
        private int currOverlap = -1;

        public void accept(TimeBoundary<SSTableStats> tb)
        {
            if (tb.isStartBoundary())
            {
                ++currOverlap;
                currentSet.add(tb.context);
                if (currOverlap > maxOverlap)
                {
                    maxOverlap = currOverlap;
                    set.clear();
                    set.addAll(currentSet);
                }
            }
            else
            {
                --currOverlap;
                currentSet.remove(tb.context);
            }
        }
    }

    static OverlappingSet getMaxOverlappingSSTables(Collection<SSTableStats> sstables)
    {
        return reduceOverTimeBoundaries(sstables, new MaxOverlappingSSTablesState(), state -> {
            TimeWindow tw = TimeWindow.merge(state.set.stream().map(s -> s.timeWindowMs).collect(Collectors.toList()));
            return new OverlappingSet(state.set, tw, state.maxOverlap);
        });
    }

    static List<SSTableStats> getOverlappingSSTables(TimeWindow timeWindowMs, Collection<SSTableStats> sstables)
    {
        return sstables.stream().filter(s -> s.timeWindowMs.intersects(timeWindowMs)).collect(Collectors.toList());
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

    private static <T> List<T> limit(int size, Collection<T> list)
    {
        return list.stream().limit(size).collect(Collectors.toList());
    }

    static String idString(Collection<SSTableStats> sstables)
    {
        return "[" + sstables.stream().map(TimeOrderedKeyCompactionStrategy::toIdStr).collect(Collectors.joining(";")) + "]";
    }

    private static String toIdStr(SSTableStats sstable)
    {
        return String.valueOf(sstable.sstable.descriptor.generation);
    }

    static String idWithTimeRange(Collection<SSTableStats> sstables)
    {
        return "[" + sstables.stream().map(TimeOrderedKeyCompactionStrategy::toIdTimeRangeStr).collect(Collectors.joining(";")) + "]";
    }

    private static String toIdTimeRangeStr(SSTableStats sstable)
    {
        return toIdStr(sstable) + "(" + sstable.sstable.getSSTableMetadata().minKey + "-" + sstable.sstable.getSSTableMetadata().maxKey + ")";
    }

    static <T> Optional<T> firstOf(Supplier<Optional<T>>... suppliers)
    {
        for (Supplier<Optional<T>> s : suppliers)
        {
            Optional<T> optionalValue = s.get();
            if (optionalValue.isPresent())
            {
                return optionalValue;
            }
        }
        return Optional.empty();
    }
}
