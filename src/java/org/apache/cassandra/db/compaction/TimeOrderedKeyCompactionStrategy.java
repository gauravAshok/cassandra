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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
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
    private long lastExpiredCheck;
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

    protected static class SSTablesForCompactionTask {
        final List<SSTableReader> sstables;
        final boolean tombstoneMerge;
        final boolean splitSStable;

        static final SSTablesForCompactionTask EMPTY = new SSTablesForCompactionTask(Collections.emptyList(), false);

        public SSTablesForCompactionTask(List<SSTableReader> sstables, boolean tombstoneMerge)
        {
            this(sstables, tombstoneMerge, true);
        }

        public SSTablesForCompactionTask(List<SSTableReader> sstables, boolean tombstoneMerge, boolean splitSStable) {
            this.sstables = sstables;
            this.tombstoneMerge = tombstoneMerge;
            this.splitSStable = splitSStable;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SSTablesForCompactionTask that = (SSTablesForCompactionTask) o;
            return sstables.equals(that.sstables);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sstables);
        }
    }

    protected static class WindowedSStablesStats {
        List<SSTableReader> tombstoneSStables = new ArrayList<>();
        List<SSTableReader> dataSStables = new ArrayList<>();
        long tombstoneCount = 0;
        long dataRowCount = 0;
        long dataSizeOnDisk = 0;
        long estimatedGarbage = 0;
    }

    protected static class SSTablesStats {
        Map<Long, WindowedSStablesStats> windowedStats = new HashMap<>();
        long totalTombstoneCount = 0;
        long totalDataRowCount = 0;
        long totalDataSizeOnDisk = 0;
        long totalEstimatedGarbage = 0;
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {

        SSTablesForCompactionTask previousCandidate = null;
        while (true)
        {
            SSTablesForCompactionTask latestBucket = getNextBackgroundSSTables(gcBefore);

            if (latestBucket == SSTablesForCompactionTask.EMPTY)
            {
                return null;
            }

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy
            // manager
            if (latestBucket.equals(previousCandidate))
            {
                logger.warn(
                        "Could not acquire references for compacting SSTables {} which is not a problem per se,"
                                + "unless it happens frequently, in which case it must be reported. Will retry later.",
                        latestBucket);
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(latestBucket.sstables, OperationType.COMPACTION);
            if (modifier != null)
            {
                return buildCompactionTask(latestBucket, modifier, gcBefore);
            }
            previousCandidate = latestBucket;
        }
    }

    private AbstractCompactionTask buildCompactionTask(SSTablesForCompactionTask compactionTask, LifecycleTransaction modifier, int gcBefore)
    {
        return new TimeOrderedKeyCompactionTask(cfs, modifier, gcBefore, twcsOptions);
    }

    /**
     * Order Of choosing sstables for compaction. The sstable will split the compacted data into multiple sstables based on time window.
     * 1. Split all the tombstone sstables created in last window.
     * 2. Estimate the potential of freeing disk space per time window.
     *    Per window do:
     *    * Get the tombstone count
     *    * Get the overlapped data tombstones and their avg size per row
     *    * Estimate the potential free space
     *    Pick the window with the max potential > per_window_free_threshold (MB) or when the total potential of free space > total_free_threashold (MB)
     * 3. Start compacting tombstone files together to reduce the number of files.
     * @param gcBefore
     * @return
     */
    private synchronized SSTablesForCompactionTask getNextBackgroundSSTables(final int gcBefore)
    {

        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
        {
            return SSTablesForCompactionTask.EMPTY;
        }

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));
        long windowSizeInSec = TimeUnit.SECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
        long newTombstoneCompactionDelayInSec = TimeUnit.SECONDS.convert(options.tombstoneCompactionDelay, options.tombstoneCompactionDelayUnit);

        // get the tombstone files created in last window
        long now = FBUtilities.nowInSeconds();
        long nowWindowForNewTombstoneCompaction = toWindow(now, newTombstoneCompactionDelayInSec);


        List<SSTableReader> tombstoneSStables = uncompacting.stream().filter(s -> s.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL).collect(Collectors.toList());

        List<SSTableReader> wideTombstoneSStables = tombstoneSStables.stream().
                filter(s ->
                        (s.maxDataAge / 1000) < nowWindowForNewTombstoneCompaction &&
                                getWindow(s, windowSizeInSec).right > 1)
                .collect(Collectors.toList());

        if(!wideTombstoneSStables.isEmpty()) {
            return new SSTablesForCompactionTask(wideTombstoneSStables, true);
        }

        List<SSTableReader> fullyExpired = getFullyExpiredSStables(tombstoneSStables, gcBefore);
        SSTablesStats ssTablesStats = buildPerWindowSStablesStats(fullyExpired, windowSizeInSec);
        populateGlobalStats(ssTablesStats);

        Optional<Map.Entry<Long, WindowedSStablesStats>> maxGarbageWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().estimatedGarbage));

        // TODO: might have to consider the situation when we are close to disk full

        if(maxGarbageWindow.isPresent()) {
            WindowedSStablesStats stats = maxGarbageWindow.get().getValue();
            long threshold = Long.min((long)(stats.dataSizeOnDisk * options.windowCompactionSizePercent), options.windowCompactionSizeInMB * 1024 * 1024);

            long globalThreshold = Long.min((long)(ssTablesStats.totalDataSizeOnDisk * options.windowCompactionGlobalSizePercent), options.windowCompactionGlobalSizeInMB * 1024 * 1024);

            // if we are breaching the per window or global garbage threshold, compact it.
            if(stats.estimatedGarbage >= threshold || ssTablesStats.totalEstimatedGarbage >= globalThreshold) {

                List<SSTableReader> candidates = new ArrayList<>(stats.tombstoneSStables);
                candidates.addAll(stats.dataSStables);
                return new SSTablesForCompactionTask(candidates, false);
            }
        }

        // no substantial garbage found, just merge some tombstone files to reduce number of sstables
        Optional<Map.Entry<Long, WindowedSStablesStats>> fragmentedWindow =
                ssTablesStats.windowedStats.entrySet().stream().max(Comparator.comparingLong(x -> x.getValue().tombstoneSStables.size()));

        if(fragmentedWindow.isPresent()) {
            return new SSTablesForCompactionTask(new ArrayList<>(fragmentedWindow.get().getValue().tombstoneSStables), true, false);
        }

        return SSTablesForCompactionTask.EMPTY;
    }

    protected SSTablesStats buildPerWindowSStablesStats(List<SSTableReader> fullyExpired, long windowSizeInSec) {
        SSTablesStats stats = new SSTablesStats();
        List<ComparablePair<Long, Long>> windows = fullyExpired.stream().map(s -> getWindow(s, windowSizeInSec)).collect(Collectors.toList());

        Set<Long> windowsWithWideSStables = new HashSet<>();
        for(int i = 0; i < fullyExpired.size(); ++i) {
            SSTableReader expired = fullyExpired.get(i);
            ComparablePair<Long, Long> window = windows.get(i);

            // window size is 1, add it to stats
            if(window.right == 1 && !windowsWithWideSStables.contains(window.left)) {
                WindowedSStablesStats perWindowStats = stats.windowedStats.get(window.left);
                if(perWindowStats == null) {
                    perWindowStats = new WindowedSStablesStats();
                }

                perWindowStats.tombstoneSStables.add(expired);
                perWindowStats.tombstoneCount += expired.getTotalRows();
            } else {
                for(int j = 0; j < window.right; ++j) {
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
            for(SSTableReader sstable: overlappedSStables) {
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

    protected void populateGlobalStats(SSTablesStats stats) {
        long totalDataSize = 0;
        long totalRowCount = 0;
        long tombstoneCount = 0;

        List<SSTableReader> allSStables = new ArrayList<>(cfs.getTracker().getView().liveSSTables());

        for(SSTableReader sstable: allSStables) {
            if(sstable.getSSTableLevel() == Memtable.DATA_SSTABLE_LVL)
            {
                totalDataSize += sstable.onDiskLength();
                totalRowCount += sstable.getTotalRows();
            }
            else if(sstable.getSSTableLevel() == Memtable.TOMBSTONE_SSTABLE_LVL) {
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

    static long toWindow(long timeInSec, long windowSizeInSec) {
        return (timeInSec / windowSizeInSec) * windowSizeInSec;
    }

    static long getTimeInSecFromClusterigKey(ByteBuffer buffer)
    {
        return ByteBufferUtil.toLong(buffer) / 1000;
    }

    private void updateEstimatedCompactionsByTasks(Iterable<SSTableReader> sstables)
    {
//        int n = 0;
//        long now = this.highestWindowSeen;
//
//        for(SSTableReader sstable: sstables) {
//            if(sstable.getSSTableMetadata().sstableLevel == )
//        }
//
//        for (ComparablePair<Integer, Long> key : tasks.keySet())
//        {
//            int windowSize = key.left;
//            if (windowSize > 2)
//            {
//                // if its a wide sstable, we need to split it
//                n++;
//            }
//            else if (windowSize == 1 && tasks.get(key).size() > 1)
//            {
//                // if multiple ss tables are there in a single window, we need to join it.
//                n += tasks.get(key).size();
//            }
//        }
//        this.estimatedRemainingTasks = n;
    }

    /**
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(HashMultimap<ComparablePair<Integer, Long>, SSTableReader> buckets, int minThreshold, int maxThreshold, long now)
    {

        // List of (window start, sstables)
        List<Pair<Long, Set<SSTableReader>>> fragmentedBucketsSorted = new ArrayList<>();
        for (ComparablePair<Integer, Long> key : buckets.keySet())
        {
            if (key.left == 1 && buckets.get(key).size() > 1)
            {
                fragmentedBucketsSorted.add(Pair.create(key.right, buckets.get(key)));
            }
        }
        fragmentedBucketsSorted.sort(Comparator.comparingInt(p -> p.right.size()));

        for (Pair<Long, Set<SSTableReader>> item : fragmentedBucketsSorted)
        {
            Long key = item.left;
            Set<SSTableReader> sstables = item.right;

            logger.trace("Key {}, sstables {}, now {}", key, sstables.size(), now);
            // TODO: use better approach
            // We can choose to do STCS for the window. After some time we can choose to do full
            // compaction on the same window to merge all sstables and hopefully
            // freeing up some space due to accumulated tombstones.
            if (sstables.size() > minThreshold)
            {
                return new ArrayList<>(sstables);
            }
        }

        // (window width, window start)
        TreeSet<ComparablePair<Integer, Long>> wideKeys = new TreeSet<>(buckets.keys().stream().filter(p -> p.left > 1).collect(Collectors.toList()));
        Iterator<ComparablePair<Integer, Long>> wideKeyIt = wideKeys.descendingIterator();
        if (wideKeyIt.hasNext())
        {
            ComparablePair<Integer, Long> key = wideKeyIt.next();
            Set<SSTableReader> bucket = buckets.get(key);
            logger.trace("Key {}, sstables {}, now {}", key, bucket.size(), now);
            return new ArrayList<>(bucket);
        }

        return Collections.emptyList();
    }

    /**
     * @param bucket       set of sstables
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the maxThreshold newest sstables.
     */
    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(Set<SSTableReader> bucket, int maxThreshold)
    {

        List<SSTableReader> ssTableReaders = new ArrayList<>(bucket);

        // Trim the largest sstables off the end to meet the maxThreshold
        Collections.sort(ssTableReaders, SSTableReader.sizeComparator);

        return ImmutableList.copyOf(Iterables.limit(ssTableReaders, maxThreshold));
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

        return new TimeWindowCompactionTask(cfs, modifier, gcBefore, twcsOptions.ignoreOverlaps).setUserDefined(true);
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
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

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

    private static class SSTableWithClusteringKeyRange
    {
        final long min;
        final long max;
        final SSTableReader sstable;

        SSTableWithClusteringKeyRange(SSTableReader sstable)
        {
            this.sstable = sstable;
            this.min = ByteBufferUtil.toLong(sstable.getSSTableMetadata().minClusteringValues.get(0));
            this.max = ByteBufferUtil.toLong(sstable.getSSTableMetadata().maxClusteringValues.get(0));
        }
    }
}
