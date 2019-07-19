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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ComparablePair;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
    private long highestWindowSeen;

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
    private synchronized SSTablesForCompaction getNextBackgroundSSTables(final int gcBefore)
    {

        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
        {
            return SSTablesForCompaction.EMPTY;
        }

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));
        long windowSizeInSec = windowSizeInSec();
        long newTombstoneCompactionDelayInSec = tombstoneComactionDelayInSec();

        // get the tombstone files created in last window
        long now = FBUtilities.nowInSeconds();
        long nowWindowForNewTombstoneCompaction = toWindow(now, newTombstoneCompactionDelayInSec);


        List<SSTableReader> tombstoneSStables = uncompacting.stream().filter(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL).collect(Collectors.toList());

        List<SSTableReader> wideTombstoneSStables = tombstoneSStables.stream().
                filter(s ->
                        (s.maxDataAge / 1000) < nowWindowForNewTombstoneCompaction &&
                                getWindow(s, windowSizeInSec).right > 1)
                .collect(Collectors.toList());

        if (!wideTombstoneSStables.isEmpty())
        {
            return new SSTablesForCompaction(wideTombstoneSStables, true, true);
        }

        List<SSTableReader> fullyExpired = getFullyExpiredSStables(tombstoneSStables, gcBefore);
        SSTablesStats ssTablesStats = buildPerWindowSStablesStats(fullyExpired, windowSizeInSec);
        populateGlobalStats(ssTablesStats);

        updateEstimatedCompactionsByTasks(ssTablesStats);

        Optional<Map.Entry<Long, WindowedSStablesStats>> maxGarbageWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().estimatedGarbage));

        if (maxGarbageWindow.isPresent())
        {
            WindowedSStablesStats stats = maxGarbageWindow.get().getValue();
            long threshold = Long.min((long) (stats.dataSizeOnDisk * options.windowCompactionSizePercent), options.windowCompactionSizeInMB * 1024 * 1024);

            long globalThreshold = Long.min((long) (ssTablesStats.totalDataSizeOnDisk * options.windowCompactionGlobalSizePercent), options.windowCompactionGlobalSizeInMB * 1024 * 1024);

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

    protected SSTablesStats buildPerWindowSStablesStats(List<SSTableReader> fullyExpired, long windowSizeInSec)
    {
        SSTablesStats stats = new SSTablesStats();
        List<ComparablePair<Long, Long>> windows = fullyExpired.stream().map(s -> getWindow(s, windowSizeInSec)).collect(Collectors.toList());

        Set<Long> windowsWithWideSStables = new HashSet<>();
        for (int i = 0; i < fullyExpired.size(); ++i)
        {
            SSTableReader expired = fullyExpired.get(i);
            ComparablePair<Long, Long> window = windows.get(i);

            // window size is 1, add it to stats
            if (window.right == 1 && !windowsWithWideSStables.contains(window.left))
            {
                WindowedSStablesStats perWindowStats = stats.windowedStats.get(window.left);
                if (perWindowStats == null)
                {
                    perWindowStats = new WindowedSStablesStats();
                }

                perWindowStats.tombstoneSStables.add(expired);
                perWindowStats.tombstoneCount += expired.getTotalRows();
            }
            else
            {
                for (int j = 0; j < window.right; ++j)
                {
                    Long wideSStablesWindow = window.left + (j * windowSizeInSec);
                    windowsWithWideSStables.add(wideSStablesWindow);
                    stats.windowedStats.remove(wideSStablesWindow);
                }
            }
        }

        // at this poing we have all the windows where tombstones files are there.
        // now get the data size metrics

        stats.windowedStats.forEach((key, value) -> {
            List<SSTableReader> overlappedSStables = getOverlappingLiveSSTables(value.tombstoneSStables)
                    .stream().filter(s -> s.getSSTableLevel() == Memtable.DATA_SSTABLE_LVL).collect(Collectors.toList());

            long totalDataSize = 0;
            long totalRowCount = 0;
            for (SSTableReader sstable : overlappedSStables)
            {
                totalDataSize += sstable.onDiskLength();
                totalRowCount += sstable.getTotalRows();
            }

            value.dataSStables = overlappedSStables;
            value.dataRowCount = totalRowCount;
            value.dataSizeOnDisk = totalDataSize;
            value.estimatedGarbage = (long) (((double) value.tombstoneCount / (double) value.dataRowCount) * value.dataSizeOnDisk);
        });

        return stats;
    }

    protected void populateGlobalStats(SSTablesStats stats)
    {
        long totalDataSize = 0;
        long totalRowCount = 0;
        long tombstoneCount = 0;

        List<SSTableReader> allSStables = new ArrayList<>(cfs.getTracker().getView().liveSSTables());

        for (SSTableReader sstable : allSStables)
        {
            if (sstable.getSSTableLevel() == Memtable.DATA_SSTABLE_LVL)
            {
                totalDataSize += sstable.onDiskLength();
                totalRowCount += sstable.getTotalRows();
            }
            else if (sstable.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL)
            {
                tombstoneCount += sstable.getTotalRows();
            }
        }

        stats.totalDataRowCount = totalRowCount;
        stats.totalDataSizeOnDisk = totalDataSize;
        stats.totalTombstoneCount = tombstoneCount;
        stats.totalEstimatedGarbage = (long) (((double) tombstoneCount / (double) totalRowCount) * totalDataSize);
    }

    protected List<SSTableReader> getFullyExpiredSStables(final Iterable<SSTableReader> uncompacting, final int gcBefore)
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

    protected Set<SSTableReader> getOverlappingLiveSSTables(final Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {} based on time in ck", sstables);

        if (!sstables.iterator().hasNext())
        {
            return ImmutableSet.of();
        }

        View view = cfs.getTracker().getView();

        List<SSTableWithClusteringKeyRange> sortedByCK = new ArrayList<>();
        sstables.forEach(s -> sortedByCK.add(new SSTableWithClusteringKeyRange(s)));
        sortedByCK.sort(Comparator.comparingLong(s -> s.min));

        List<Pair<Long, Long>> bounds = new ArrayList<>();
        long first = 0, last = Long.MAX_VALUE;

        for (SSTableWithClusteringKeyRange sstable : sortedByCK)
        {
            if (first == 0)
            {
                first = sstable.min;
                last = sstable.max;
            }
            else
            {
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
        }
        bounds.add(Pair.create(first, last));
        Set<SSTableReader> results = new HashSet<>();

        for (Pair<Long, Long> bound : bounds)
        {
            Iterables.addAll(results, view.liveSSTablesInTimeRange(bound.left, bound.right));
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
        long minTime = getTimeInSecFromClusterigKey(metadata.minClusteringValues.get(0));
        long maxTime = getTimeInSecFromClusterigKey(metadata.maxClusteringValues.get(0));

        return rangeToWindow(minTime, maxTime, windowSizeInSec);
    }

    static ComparablePair<Long, Long> rangeToWindow(long tsStart, long tsEnd, long windowSizeInSec)
    {
        long lowerWindowBound = toWindow(tsStart, windowSizeInSec);
        long upperWindowBound = toWindow(tsEnd, windowSizeInSec) + windowSizeInSec;

        return ComparablePair.create(lowerWindowBound, (upperWindowBound - lowerWindowBound) / windowSizeInSec);
    }

    static long toWindow(long timeInSec, long windowSizeInSec)
    {
        return (timeInSec / windowSizeInSec) * windowSizeInSec;
    }

    static long getTimeInSecFromClusterigKey(ByteBuffer buffer)
    {
        return ByteBufferUtil.toLong(buffer) / 1000;
    }

    private void updateEstimatedCompactionsByTasks(SSTablesStats stats)
    {
        int n = stats.windowedStats.size();

        long now = FBUtilities.nowInSeconds();
        long windowSizeInSec = windowSizeInSec();
        long currentWindow = toWindow(now, windowSizeInSec);
        long tombstoneDelayedWindow = toWindow(now, tombstoneComactionDelayInSec());

        int pendingWindows = (int) ((tombstoneDelayedWindow - currentWindow) / windowSizeInSec);

        n += pendingWindows;

        this.estimatedRemainingTasks = n;
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        return null;
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

        boolean onylTombstones = sstables.stream().allMatch(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL);

        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, twcsOptions, true, onylTombstones).setUserDefined(true);
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

    private long tombstoneComactionDelayInSec()
    {
        return TimeUnit.SECONDS.convert(options.tombstoneCompactionDelay, options.tombstoneCompactionDelayUnit);
    }

    private static class SSTableWithClusteringKeyRange
    {
        final SSTableReader sstable;
        final long min;
        final long max;

        SSTableWithClusteringKeyRange(SSTableReader sstable)
        {
            this.sstable = sstable;
            this.min = ByteBufferUtil.toLong(sstable.getSSTableMetadata().minClusteringValues.get(0));
            this.max = ByteBufferUtil.toLong(sstable.getSSTableMetadata().maxClusteringValues.get(0));
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
    }

    protected static class WindowedSStablesStats
    {
        List<SSTableReader> tombstoneSStables = new ArrayList<>();
        List<SSTableReader> dataSStables = new ArrayList<>();
        long tombstoneCount = 0;
        long dataRowCount = 0;
        long dataSizeOnDisk = 0;
        long estimatedGarbage = 0;
    }

    protected static class SSTablesStats
    {
        Map<Long, WindowedSStablesStats> windowedStats = new HashMap<>();
        long totalTombstoneCount = 0;
        long totalDataRowCount = 0;
        long totalDataSizeOnDisk = 0;
        long totalEstimatedGarbage = 0;
    }
}
