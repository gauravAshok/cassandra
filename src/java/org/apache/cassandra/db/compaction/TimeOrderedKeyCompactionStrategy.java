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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.ComparablePair;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Set<SSTableReader> sstables = new HashSet<>();

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
            logger.debug("Disabling tombstone compactions for PKTWCS");
        }
        else
        {
            logger.debug("Enabling tombstone compactions for PKTWCS");
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
                if (logger.isDebugEnabled())
                {
                    logger.debug("compaction task: {}", sstables);
                }
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
     * @param gcBefore
     * @return
     */
    synchronized SSTablesForCompaction getNextBackgroundSSTables(final int gcBefore)
    {
        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
        {
            return SSTablesForCompaction.EMPTY;
        }

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains)));
        long windowSizeInSec = windowSizeInSec();
        long newTombstoneCompactionDelayInSec = tombstoneCompactionDelayInSec();

        // get the tombstone files created in last window
        long now = FBUtilities.nowInSeconds();
        long nowWindowForNewTombstoneCompaction = toWindow(now, newTombstoneCompactionDelayInSec);

        List<SSTableReader> tombstoneSStables = uncompacting.stream()
                .filter(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL)
                .filter(s -> (s.getSSTableMetadata().maxKey / 1000) < nowWindowForNewTombstoneCompaction)
                .collect(Collectors.toList());

        List<SSTableReader> wideTombstoneSStables = tombstoneSStables.stream().
                filter(s -> getWindow(s, windowSizeInSec).right > 1)
                .collect(Collectors.toList());

        if (!wideTombstoneSStables.isEmpty())
        {
            return new SSTablesForCompaction(wideTombstoneSStables, true, true);
        }

        return getSSTablesForCompaction(gcBefore, tombstoneSStables);
    }

    protected SSTablesForCompaction getSSTablesForCompaction(int gcBefore, List<SSTableReader> tombstoneSStables)
    {
        long windowSizeInSec = windowSizeInSec();

        List<SSTableReader> fullyExpired = getFullyExpiredSStables(tombstoneSStables, gcBefore);
        SSTablesStats ssTablesStats = buildPerWindowSStablesStats(cfs, fullyExpired, windowSizeInSec);
        populateGlobalStats(cfs, ssTablesStats);

        updateEstimatedCompactionsByTasks(ssTablesStats);

        Optional<Map.Entry<Long, WindowedSStablesStats>> maxGarbageWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().estimatedGarbage));

        if (maxGarbageWindow.isPresent())
        {
            WindowedSStablesStats stats = maxGarbageWindow.get().getValue();
            long threshold = Long.min((long) (stats.dataSizeOnDisk * (options.windowCompactionSizePercent / 100.0)), options.windowCompactionSizeInMB * 1024 * 1024);

            long globalThreshold = Long.min((long) (ssTablesStats.totalDataSizeOnDisk * (options.windowCompactionGlobalSizePercent / 100.0)), options.windowCompactionGlobalSizeInMB * 1024 * 1024);

            // if we are breaching the per window or global garbage threshold, compact it.
            if (stats.estimatedGarbage >= threshold || ssTablesStats.totalEstimatedGarbage >= globalThreshold)
            {
                List<SSTableReader> candidates = new ArrayList<>(stats.tombstoneSStables);
                candidates.addAll(stats.dataSStables);
                return new SSTablesForCompaction(candidates, false, true);
            }
        }

        // no substantial garbage found, just merge some tombstone files to reduce number of sstables
        Optional<Map.Entry<Long, WindowedSStablesStats>> fragmentedWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().tombstoneSStables.size()));

        if (fragmentedWindow.isPresent())
        {
            return new SSTablesForCompaction(new ArrayList<>(fragmentedWindow.get().getValue().tombstoneSStables), true, false);
        }

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
                logger.trace("Dropping overlap ignored expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
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
        sortedByKey.sort(Comparator.comparingLong(s -> s.min));

        List<Pair<Long, Long>> bounds = new ArrayList<>();
        long first = 0, last = Long.MAX_VALUE;

        Iterator<SSTableWithKeyRange> it = sortedByKey.iterator();
        if(it.hasNext()) {
            SSTableWithKeyRange sstable = it.next();
            first = sstable.min;
            last = sstable.max;
        }

        while(it.hasNext()) {
            SSTableWithKeyRange sstable = it.next();
            if (sstable.min <= last) // we do overlap
            {
                if (sstable.max > last)
                {
                    last = sstable.max;
                }
            }
            else
            {
                bounds.add(Pair.create(first, last));
                first = sstable.min;
                last = sstable.max;
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

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    static ComparablePair<Long, Long> getWindow(SSTableReader ssTableReader, long windowSizeInSec)
    {
        StatsMetadata metadata = ssTableReader.getSSTableMetadata();
        long minTime = metadata.minKey / 1000;
        long maxTime = metadata.maxKey / 1000;

        return rangeToWindow(minTime, maxTime, windowSizeInSec);
    }

    static ComparablePair<Long, Long> rangeToWindow(long tsStartInclusive, long tsEndExlusive, long windowSizeInSec)
    {
        long lowerWindowBound = toWindow(tsStartInclusive, windowSizeInSec);
        long upperWindowBound = toWindow(tsEndExlusive, windowSizeInSec) + (tsEndExlusive % windowSizeInSec == 0 ? 0 : windowSizeInSec);

        return ComparablePair.create(lowerWindowBound, (upperWindowBound - lowerWindowBound) / windowSizeInSec);
    }

    static long toWindow(long timeInSec, long windowSizeInSec)
    {
        return (timeInSec / windowSizeInSec) * windowSizeInSec;
    }

    private void updateEstimatedCompactionsByTasks(SSTablesStats stats)
    {
        int n = stats.windowedStats.size();

        long now = FBUtilities.nowInSeconds();
        long windowSizeInSec = windowSizeInSec();
        long currentWindow = toWindow(now, windowSizeInSec);
        long tombstoneDelayedWindow = toWindow(now, tombstoneCompactionDelayInSec());

        int pendingWindows = (int) ((tombstoneDelayedWindow - currentWindow) / windowSizeInSec);

        n += pendingWindows;

        this.estimatedRemainingTasks = n;
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

        boolean onlyTombstones = sstables.stream().allMatch(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL);

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
        return String.format("TimeOrderedKeyCompactionStrategy[%s/%s]", cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return false;
    }

    private long windowSizeInSec()
    {
        return TimeUnit.SECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
    }

    private long tombstoneCompactionDelayInSec()
    {
        return TimeUnit.SECONDS.convert(options.tombstoneCompactionDelay, options.tombstoneCompactionDelayUnit);
    }

    private static class SSTableWithKeyRange
    {
        final SSTableReader sstable;
        final long min;
        final long max;

        SSTableWithKeyRange(SSTableReader sstable)
        {
            this.sstable = sstable;
            this.min = sstable.getSSTableMetadata().minKey;
            this.max = sstable.getSSTableMetadata().maxKey;
        }
    }

    protected static class SSTablesForCompaction
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
            return "SSTablesForCompaction{" +
                    "\nsstables=" + sstables +
                    ",\n tombstoneMerge=" + tombstoneMerge +
                    ",\n splitSStable=" + splitSStable +
                    "\n}";
        }
    }

    protected static class WindowedSStablesStats
    {
        List<SSTableReader> tombstoneSStables = new ArrayList<>();
        List<SSTableReader> dataSStables = new ArrayList<>();
        long dataSizeOnDisk = 0;
        long estimatedGarbage = 0;

        @Override
        public String toString()
        {
            return "WindowedSStablesStats{" +
                    "tombstoneSStables=" + tombstoneSStables +
                    ", dataSStables=" + dataSStables +
                    ", dataSizeOnDisk=" + dataSizeOnDisk +
                    ", estimatedGarbage=" + estimatedGarbage +
                    '}';
        }
    }

    protected static class SSTablesStats
    {
        Map<Long, WindowedSStablesStats> windowedStats = new HashMap<>();
        long totalDataSizeOnDisk = 0;
        long totalEstimatedGarbage = 0;

        @Override
        public String toString()
        {
            return "SSTablesStats{" +
                    "windowedStats=" + windowedStats +
                    ", totalDataSizeOnDisk=" + totalDataSizeOnDisk +
                    ", totalEstimatedGarbage=" + totalEstimatedGarbage +
                    '}';
        }
    }

    private static class TombstoneCounts
    {
        long partitionTombstones = 0;
        long rowTombstones = 0;
        long rangeTombstones = 0;
    }
}
