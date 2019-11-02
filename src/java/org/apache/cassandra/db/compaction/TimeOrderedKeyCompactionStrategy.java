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
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.ComparablePair;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            SSTablesForCompaction sstables = getNextBackgroundSSTables(gcBefore);

            if (sstables == SSTablesForCompaction.EMPTY)
            {
                return null;
            }

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy
            // manager
            if (sstables.equals(previousCandidate))
            {
                logger.warn(
                        "Could not acquire references for compacting SSTables {} which is not a problem per se,"
                                + "unless it happens frequently, in which case it must be reported. Will retry later.",
                        sstables);
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables.sstables, OperationType.COMPACTION);
            if (modifier != null)
            {
                logger.info("compaction task: {}", sstables);
                return buildCompactionTask(sstables, modifier, gcBefore);
            }
            previousCandidate = sstables;
        }
    }

    private AbstractCompactionTask buildCompactionTask(SSTablesForCompaction sstables, LifecycleTransaction modifier, int gcBefore)
    {
        gcBefore = sstables.tombstoneMerge ? CompactionManager.NO_GC : gcBefore;
        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, twcsOptions, sstables.splitSStable, sstables.tombstoneMerge);
    }

    /**
     * Outdated:
     * Order Of choosing sstables for compaction.
     * 1. Split, based on time window, all the tombstone sstables created in last window.
     * 2. Estimate the potential garbage per time window.
     * Per window do:
     * * Get the tombstone count
     * * Get the overlapped data tombstones and their avg size per row
     * * Estimate the potential garbage
     * Pick the window with the max potential > per_window_free_threshold (MB) or when the total potential of free space > total_free_threashold (MB)
     * 3. Start compacting tombstone files together to reduce the number of files.
     *
     * There were issues with the above approach,
     * 1. Splitting of tombstone files into smaller ones was unconstrained and could lead to numerous tiny files to be created in one compaction task.
     * 2. Tiny files created still would not get deleted because of the longer gc_grace_seconds. So on every flush, we will get small files per window and then
     *    the compaction will have to merge them to keep the # files lower.
     *
     * Newer approach:
     * 1.
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
        int windowSizeInSec = windowSizeInSec();
        int gcGraceSeconds = cfs.metadata.params.gcGraceSeconds;

        long now = FBUtilities.nowInSeconds();

        // get the tombstone files created in last window
        long currentGcWindow = toWindow(now, gcGraceSeconds);

        // tombstone sstables that have expired and now can be compacted with data sstables to free up space.
        List<SSTableStats> compactableTombstones = new ArrayList(uncompacting.size());
        // tombstones sstables are only created recently and cannot be deleted. These will be meregd together to keep the overlap between sstables low.
        List<SSTableStats> latestTombstones = new ArrayList<>();
        // tombstone sstables that are about to be expired. They most likely will have 0 overlap. But if not (due to streaming), then merge it.
        List<SSTableStats> nearExpiryTombstones = new ArrayList<>();
        // data sstables
        List<SSTableStats> dataSStables = new ArrayList<>();

        uncompacting.forEach(s -> {
            if (s.getSSTableLevel() == SSTable.TOMBSTONE_SSTABLE_LVL)
            {
                if (toWindow(s.getSSTableMetadata().maxLocalDeletionTime, gcGraceSeconds) >= currentGcWindow)
                {
                    latestTombstones.add(sstables.get(s));
                }
                else if (s.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                {
                    compactableTombstones.add(sstables.get(s));
                }
                else
                {
                    nearExpiryTombstones.add(sstables.get(s));
                }
            }
            else {

            }
        });

        // look for garbage cleanup.
        List<OverlappingSet> overlappingTombstones = distinctOverlappingSets(compactableTombstones);


        // TODO: maybe look at the data sstables too. streaming to new node can lead to fragmented data sstables which may need compaction (verify). Explore!!
        return getSSTablesForCompaction(gcBefore, tombstoneSStables);
    }

    protected SSTablesForCompaction getSSTablesForCompaction(int gcBefore, List<SSTableReader> tombstoneSStables)
    {
        long windowSizeInSec = windowSizeInSec();

        List<SSTableReader> fullyExpired = getFullyExpiredSStables(tombstoneSStables, gcBefore);
        SSTablesStats ssTablesStats = buildPerWindowSStablesStats(cfs, fullyExpired, windowSizeInSec);
        populateGlobalStats(cfs, ssTablesStats);

        // TODO: maybe too late to estimate the remaining tasks. When compacting latest tombstones, we wont get the chance to update this. Correct it!!!
        updateEstimatedCompactionsByTasks(ssTablesStats);

        Optional<Map.Entry<Long, WindowedSStablesStats>> maxGarbageWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().estimatedGarbage));

        if (maxGarbageWindow.isPresent())
        {
            WindowedSStablesStats stats = maxGarbageWindow.get().getValue();
            if (!stats.tombstoneSStables.isEmpty())
            {
                Pair<Long, Long> threshold = getGarbageThreshold(ssTablesStats.totalDataSizeOnDisk, stats.dataSizeOnDisk);

                logger.info("garbage check: {}/{} MB, threshold: {}/{} MB",
                        stats.estimatedGarbage / (double) FileUtils.ONE_MB, ssTablesStats.totalEstimatedGarbage / (double) FileUtils.ONE_MB,
                        threshold.left / (double) FileUtils.ONE_MB, threshold.right / (double) FileUtils.ONE_MB);

                // if we are breaching the per window or global garbage threshold, compact it.
                if (stats.estimatedGarbage >= threshold.left || ssTablesStats.totalEstimatedGarbage >= threshold.right)
                {
                    List<SSTableReader> candidates = new ArrayList<>(stats.tombstoneSStables);
                    candidates.addAll(stats.dataSStables);
                    return new SSTablesForCompaction(candidates, false, true);
                }
            }
        }

        // no substantial garbage found, just merge some files to reduce number of sstables
        Optional<Map.Entry<Long, WindowedSStablesStats>> fragmentedWindow = mostFragmented(ssTablesStats);

        if (fragmentedWindow.isPresent())
        {
            long timeWindow = fragmentedWindow.get().getKey();
            WindowedSStablesStats stats = fragmentedWindow.get().getValue();
            logger.info("fragmentation check: {}, total: {}, data: {}", timeWindow, stats.maxFragmentation, stats.maxDataSStableFragmentation);

            List<SSTableReader> sstables = new ArrayList<>();
            sstables.addAll(stats.tombstoneSStables);
            boolean onlyTombstoneMerge = true;

            // there can always be an overlap of 1 whenever an sstable gets flushed.
            if (stats.maxDataSStableFragmentation > 1)
            {
                sstables.addAll(stats.dataSStables);
                onlyTombstoneMerge = false;
            }

            if (sstables.size() > 1)
            {
                // TODO: splitsstables should probably be false!!
                return new SSTablesForCompaction(sstables, onlyTombstoneMerge, true);
            }
        }

        logger.info("nothing found to compact");
        return SSTablesForCompaction.EMPTY;
    }

    static SSTablesStats buildPerWindowSStablesStats(ColumnFamilyStore cfs, List<SSTableReader> fullyExpired, long windowSizeInSec)
    {
        SSTablesStats stats = new SSTablesStats();

        Set<Long> windowsWithWideSStables = new HashSet<>();
        for (SSTableReader expired : fullyExpired)
        {
            ComparablePair<Long, Long> window = getWindow(expired, windowSizeInSec);

            // window size is 1, add it to stats
            if (window.right == 1 && !windowsWithWideSStables.contains(window.left))
            {
                WindowedSStablesStats perWindowStats = stats.windowedStats.get(window.left);
                if (perWindowStats == null)
                {
                    perWindowStats = new WindowedSStablesStats();
                    stats.windowedStats.put(window.left, perWindowStats);
                }

                perWindowStats.tombstoneSStables.add(expired);
            }
            else
            {
                //TODO: will we ever find wide sstables. We are prioritizing splitting wide sstable. so think again.
                for (int j = 0; j < window.right; ++j)
                {
                    Long wideSStablesWindow = window.left + (j * windowSizeInSec);
                    windowsWithWideSStables.add(wideSStablesWindow);
                    stats.windowedStats.remove(wideSStablesWindow);
                }
            }
        }

        // at this point we have all the windows where tombstones files are there.
        // now get the appropriate metrics
        stats.windowedStats.forEach((key, value) -> {
            List<SSTableReader> overlappedSStables = getOverlappingLiveSSTables(cfs, value.tombstoneSStables)
                    .stream().filter(s -> s.getSSTableLevel() == Memtable.DATA_SSTABLE_LVL).collect(Collectors.toList());

            value.dataSStables = overlappedSStables;
            value.dataSizeOnDisk = overlappedSStables.stream().mapToLong(SSTableReader::onDiskLength).sum();

            double estimatedGarbage = getEstimatedGarbage(
                    getApproxTombstoneCounts(value.tombstoneSStables),
                    SSTableReader.getApproximateKeyCount(overlappedSStables),
                    overlappedSStables.stream().mapToLong(SSTableReader::getTotalRows).sum(),
                    value.dataSizeOnDisk);

            value.estimatedGarbage = Math.min((long) estimatedGarbage, value.dataSizeOnDisk);

            // compute fragmentation stats
            value.maxDataSStableFragmentation = maxOverlap(value.dataSStables);
            value.maxFragmentation = value.maxDataSStableFragmentation + value.tombstoneSStables.size();
        });

        return stats;
    }

    static void populateGlobalStats(ColumnFamilyStore cfs, SSTablesStats stats)
    {
        List<SSTableReader> allSStables = new ArrayList<>(cfs.getTracker().getView().liveSSTables());

        List<SSTableReader> tombstones = allSStables.stream().filter(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL).collect(Collectors.toList());
        List<SSTableReader> data = allSStables.stream().filter(s -> s.getSSTableLevel() == Memtable.DATA_SSTABLE_LVL).collect(Collectors.toList());

        stats.totalDataSizeOnDisk = data.stream().mapToLong(SSTableReader::onDiskLength).sum();

        double estimatedGarbage = getEstimatedGarbage(
                getApproxTombstoneCounts(tombstones),
                SSTableReader.getApproximateKeyCount(data),
                data.stream().mapToLong(SSTableReader::getTotalRows).sum(),
                stats.totalDataSizeOnDisk);

        stats.totalEstimatedGarbage = Math.min((long) estimatedGarbage, stats.totalDataSizeOnDisk);
    }

    private static TombstoneCounts getApproxTombstoneCounts(List<SSTableReader> sstables)
    {
        TombstoneCounts count = new TombstoneCounts();
        sstables.forEach(s -> {
            StatsMetadata meta = s.getSSTableMetadata();
            count.partitionTombstones += meta.partitionTombstones;
            count.rowTombstones += meta.rowTombstones;
            count.rangeTombstones += meta.rangeTombstones;
        });
        return count;
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

    static List<SSTableReader> getFullyExpiredSStables(final Iterable<SSTableReader> uncompacting, final int gcBefore)
    {
        List<SSTableReader> fullyExpired = new ArrayList<>();
        for (SSTableReader candidate : uncompacting)
        {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
            {
                fullyExpired.add(candidate);
            }
        }

        return fullyExpired;
    }

    static Set<SSTableReader> getOverlappingLiveSSTables(ColumnFamilyStore cfs, final Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {} based on time", sstables);

        if (!sstables.iterator().hasNext())
        {
            return ImmutableSet.of();
        }

        View view = cfs.getTracker().getView();

        List<SSTableWithKeyRange> sortedByKey = new ArrayList<>();
        sstables.forEach(s -> sortedByKey.add(new SSTableWithKeyRange(s)));
        sortedByKey.sort(Comparator.naturalOrder());

        List<Pair<Long, Long>> bounds = new ArrayList<>();
        long first = 0, last = Long.MAX_VALUE;

        Iterator<SSTableWithKeyRange> it = sortedByKey.iterator();
        if (it.hasNext())
        {
            SSTableWithKeyRange sstable = it.next();
            first = sstable.left;
            last = sstable.right;
        }

        while (it.hasNext())
        {
            SSTableWithKeyRange sstable = it.next();
            if (sstable.left <= last) // we do overlap
            {
                if (sstable.right > last)
                {
                    last = sstable.right;
                }
            }
            else
            {
                bounds.add(Pair.create(first, last));
                first = sstable.left;
                last = sstable.right;
            }
        }

        bounds.add(Pair.create(first, last));
        Set<SSTableReader> results = new HashSet<>();

        for (Pair<Long, Long> bound : bounds)
        {
            Iterables.addAll(results, view.liveSSTablesInTimeRange(bound.left, bound.right - 1));
        }

        return Sets.difference(results, ImmutableSet.copyOf(sstables));
    }

    static Optional<Map.Entry<Long, WindowedSStablesStats>> mostFragmented(SSTablesStats stats)
    {
        return stats.windowedStats.entrySet().stream()
                .max(Comparator.<Map.Entry<Long, WindowedSStablesStats>>comparingInt(s -> s.getValue().maxFragmentation)
                        .thenComparingInt(s -> s.getValue().maxDataSStableFragmentation));
    }

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        sstables.put(sstable, new SSTableStats(sstable));
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

    static TimeWindow toWindow(long tsStartInclusive, long tsEndExlusive, int windowSizeInSec)
    {
        long lowerWindowBound = toWindow(tsStartInclusive, windowSizeInSec);
        long upperWindowBound = toWindow(tsEndExlusive, windowSizeInSec) + (tsEndExlusive % windowSizeInSec == 0 ? 0 : windowSizeInSec);

        return new TimeWindow(lowerWindowBound, (int) (upperWindowBound - lowerWindowBound));
    }

    static long toWindow(long timeInSec, int windowSizeInSec)
    {
        return (timeInSec / windowSizeInSec) * windowSizeInSec;
    }

    private void updateEstimatedCompactionsByTasks(SSTablesStats stats)
    {
        int n = (int) stats.windowedStats.entrySet().stream().filter(s -> isCandidateForCompaction(s.getValue())).count();
        logger.info("pending tasks: {}, total_time_windows: {}", n, stats.windowedStats.size());
        this.estimatedRemainingTasks = n;
    }

    private boolean isCandidateForCompaction(WindowedSStablesStats stat)
    {
        return !stat.tombstoneSStables.isEmpty() || stat.maxDataSStableFragmentation > 0;
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
        {
            return null;
        }
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
        {
            return null;
        }
        return Collections.singleton(new TimeOrderedKeyCompactionTask(cfs, txn, gcBefore, twcsOptions, true, false));
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

        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, twcsOptions, true, onlyTombstones).setUserDefined(true);
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

    private int windowSizeInSec()
    {
        return (int) TimeUnit.SECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
    }

    private static class SSTableWithKeyRange extends ComparablePair<Long, Long>
    {
        final SSTableReader sstable;

        SSTableWithKeyRange(SSTableReader sstable)
        {
            super(sstable.getSSTableMetadata().minKey, sstable.getSSTableMetadata().maxKey);
            this.sstable = sstable;
        }
    }

    static class SSTablesForCompaction
    {
        final List<SSTableReader> sstables;
        final boolean tombstoneMerge;
        final boolean splitSStable;

        static final SSTablesForCompaction EMPTY = new SSTablesForCompaction(Collections.emptyList(), false, true);

        public SSTablesForCompaction(List<SSTableReader> sstables, boolean tombstoneMerge, boolean splitSStable)
        {
            this.sstables = sstables;
            this.tombstoneMerge = tombstoneMerge;
            this.splitSStable = splitSStable;
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
                    ",\n splitSStable=" + splitSStable +
                    "\n}";
        }
    }

    static class WindowedSStablesStats
    {
        List<SSTableReader> tombstoneSStables = new ArrayList<>();
        List<SSTableReader> dataSStables = new ArrayList<>();
        long dataSizeOnDisk = 0;
        long estimatedGarbage = 0;
        int maxFragmentation = 0;
        int maxDataSStableFragmentation = 0;

        @Override
        public String toString()
        {
            return "WindowedSStablesStats{" +
                    "tombstoneSStables=" + tombstoneSStables +
                    ", dataSStables=" + dataSStables +
                    ", dataSizeOnDisk=" + dataSizeOnDisk +
                    ", estimatedGarbage=" + estimatedGarbage +
                    ", maxFragmentation=" + maxFragmentation +
                    ", maxDataSStableFragmentation=" + maxDataSStableFragmentation +
                    '}';
        }
    }

    private Pair<Long, Long> getGarbageThreshold(long totalSizeOnDisk, long windowDataSizeOnDisk)
    {
        return Pair.create(
                Long.min(
                        (long) (windowDataSizeOnDisk * (options.windowCompactionSizePercent / 100.0)),
                        options.windowCompactionSizeInMB * FileUtils.ONE_MB),
                Long.min(
                        (long) (totalSizeOnDisk * (options.windowCompactionGlobalSizePercent / 100.0)),
                        options.windowCompactionGlobalSizeInMB * FileUtils.ONE_MB
                ));
    }

//    static class SSTablesStats
//    {
//        Map<Long, WindowedSStablesStats> windowedStats = new HashMap<>();
//        long totalDataSizeOnDisk = 0;
//        long totalEstimatedGarbage = 0;
//
//        @Override
//        public String toString()
//        {
//            return "SSTablesStats{" +
//                    "windowedStats=" + windowedStats +
//                    ", totalDataSizeOnDisk=" + totalDataSizeOnDisk +
//                    ", totalEstimatedGarbage=" + totalEstimatedGarbage +
//                    '}';
//        }
//    }

    private static class TombstoneCounts
    {
        long partitionTombstones = 0;
        long rowTombstones = 0;
        long rangeTombstones = 0;
    }

    private static class SSTableStats
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
            return "SStableStats{" +
                    "sstable=" + sstable +
                    ", cardinality=" + cardinality +
                    ", keyCount=" + keyCount +
                    ", rowCount=" + rowCount +
                    ", tombstoneCounts=" + tombstoneCounts +
                    ", timeWindow=" + timeWindow +
                    '}';
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

    private static class OverlappingSet
    {
        List<SSTableStats> sstables = new ArrayList<>();
        TimeWindow timeWindow;
        int maxOverlap;
    }

    static List<OverlappingSet> distinctOverlappingSets(List<SSTableStats> sstables)
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
}
