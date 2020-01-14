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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.TimeOrderedKeyCompactionStrategy.SSTableStats;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeWindow;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.cassandra.db.compaction.TimeOrderedKeyCompactionStrategy.validateOptions;
import static org.junit.Assert.*;

public class TimeOrderedKeyCompactionStrategyTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.TimeOrderedKeyCFMD.standardCFMD(KEYSPACE1, CF_STANDARD1)
                                                                   .compaction(CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, TOKCSUtil.getDefaultTOKCSOptions()))
                                                                   .gcGraceSeconds(0)
                                                                   .timeOrderedKey(true));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY, "10");
        options.put(TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, "30,100");
        options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, "30,100");

        Map<String, String> unvalidated = TimeOrderedKeyCompactionStrategy.validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MONTHS");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "not-a-boolean");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "false");
        }

        try
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY));
        }
        catch (ConfigurationException e)
        {
        }

        try
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY, "10000000");
            validateOptions(options);
            fail(String.format("%s == 10000000 should be rejected", TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY, "1");
        }

        try
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY, "10");
            validateOptions(options);
        }
        catch (ConfigurationException e)
        {
            fail(String.format("%s == 10 should not be rejected", TimeOrderedKeyCompactionStrategyOptions.SPLIT_FACTOR_KEY));
        }

        String[] validThresholds = {"1,0.01", "50,50", "10000,100"};
        String[] invalidThresholds = {"-1,0.01", "50,-50", "-10000,100", "10,120"};

        for (String valid : validThresholds)
        {
            try
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, valid);
                validateOptions(options);
            }
            catch (Exception e)
            {
                fail(String.format("%s == %s should not be rejected", TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, valid));
            }
        }

        for (String invalid : invalidThresholds)
        {
            try
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, invalid);
                validateOptions(options);
                fail(String.format("%s == %s should be rejected", TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, invalid));
            }
            catch (ConfigurationException e)
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, TimeOrderedKeyCompactionStrategyOptions.DEFAULT_GARBAGE_SIZE_THRESHOLD_STR);
            }
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }


    @Test
    public void testTimeWindows()
    {
        long ts1 = epoch("2019-08-09 13:01:01");
        long ts2 = epoch("2019-08-09 13:01:59");
        long ts3 = epoch("2019-08-09 13:02:15");
        long ts4 = epoch("2019-08-09 13:03:00");
        long ts5 = epoch("2019-08-09 14:01:00");
        long ts6 = epoch("2019-08-09 18:59:59");
        long ts7 = epoch("2019-08-10 00:00:00");
        long ts8 = epoch("2019-08-10 13:01:01");
        long ts9 = epoch("2019-08-10 13:01:00");

        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:01:00"), 1 * 60_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts2, 60_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:01:00"), 3 * 30_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts3, 30_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:01:00"), 8 * 15_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts4, 15_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:01:00"), 120 * 30_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts5, 30_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:01:00"), (59 + 60 * 5) * 60_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts6, 60_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:00:00"), (60 * 11 / 2) * 120_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts7, 120_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 13:00:00"), (60 * 24 / 4 + 1) * 240_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts8, 240_000));
        Assert.assertEquals(TimeWindow.fromDuration(epoch("2019-08-09 12:56:00"), (60 * 24 / 8 + 1) * 480_000), TimeOrderedKeyCompactionStrategy.toWindowMs(ts1, ts9, 480_000));
    }

    private long epoch(String str)
    {
        return LocalDateTime.parse(str, f).toEpochSecond(ZoneOffset.UTC) * 1000;
    }

    @Test
    public void testMetadata()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        TOKCSUtil.insertStandard1(cfs, 0, 0, 0, "inserted");
        TOKCSUtil.deleteStandard1(cfs, 1, 1, 0);
        TOKCSUtil.deleteStandard1ByPartitionKey(cfs, 2, 2);
        TOKCSUtil.deleteRangeStandard1(cfs, 3, 3, 0, 10);

        cfs.forceBlockingFlush();

        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        List<SSTableReader> files = sort(cfs.getLiveSSTables());
        SSTableReader data = files.get(0), tombstone = files.get(1);

        assertEquals(1, data.getTotalRows());
        assertEquals(1, SSTableReader.getApproximateKeyCount(Collections.singletonList(data)));
        assertEquals(0, data.getSSTableMetadata().partitionTombstones);
        assertEquals(0, data.getSSTableMetadata().rowTombstones);
        assertEquals(0, data.getSSTableMetadata().rangeTombstones);

        assertEquals(1, tombstone.getTotalRows());
        assertEquals(3, SSTableReader.getApproximateKeyCount(Collections.singletonList(tombstone)));
        assertEquals(1, tombstone.getSSTableMetadata().partitionTombstones);
        assertEquals(1, tombstone.getSSTableMetadata().rowTombstones);
        assertEquals(2, tombstone.getSSTableMetadata().rangeTombstones);
    }

    /*
        TOKCS should choose the window with the max potential garbage.
        cases:
            1. 10 partition, 10 rows each, 100 row deletes
            2. 10 partition, 10 rows each, 10 row deletes, 10 partition deletes
            3. 10 partition, 10 rows each, 50 row deletes, 10 partition deletes
            4. 10 partition, 10 rows each, 10 row deletes (1 row delete per partition 0-3), 5 partition deletes (4-9)
    */

    @Test
    public void testChooseGarbageWindowCase1()
    {
        testChooseGarbageWindow(10, 10, 10, IntStream.range(0, 10), IntStream.empty(), 95.0, 100.0);
    }

    @Test
    public void testChooseGarbageWindowCase2()
    {
        testChooseGarbageWindow(10, 10, 0, IntStream.empty(), IntStream.range(0, 10), 95.0, 100.0);
    }

    @Test
    public void testChooseGarbageWindowCase3()
    {
        testChooseGarbageWindow(10, 10, 10, IntStream.range(0, 5), IntStream.range(0, 10), 95.0, 100.0);
    }

    @Test
    public void testChooseGarbageWindowCase4()
    {
        testChooseGarbageWindow(10, 10, 1, IntStream.range(0, 4), IntStream.range(4, 10), 61.0, 66.0);
    }

    public void testChooseGarbageWindow(int partitions, int rowsPerPartition, int rowDeletesPerPartition,
                                        IntStream rowDeletePartitionRange, IntStream partitionDeleteRange, double minGarbagePct, double maxGarbagePct)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        populateData(cfs, partitions, rowsPerPartition, rowDeletesPerPartition, rowDeletePartitionRange, partitionDeleteRange);

        List<SSTableReader> dataOnly = new ArrayList<>(Util.dataOnly(cfs.getLiveSSTables()));
        List<SSTableReader> tombstoneOnly = new ArrayList<>(Util.tombstonesOnly(cfs.getLiveSSTables()));
        Assert.assertEquals(1, dataOnly.size());
        Assert.assertEquals(1, tombstoneOnly.size());

        SSTableStats data = new SSTableStats(dataOnly.get(0)), tombstone = new SSTableStats(tombstoneOnly.get(0));

        // get the compaction task
        TimeOrderedKeyCompactionStrategy compactionStrategy = new TimeOrderedKeyCompactionStrategy(cfs, TOKCSUtil.getDefaultTOKCSOptions());

        TimeOrderedKeyCompactionStrategy.CategorizedSSTables categorized = compactionStrategy.categorizeSStables(cfs.getLiveSSTables(), FBUtilities.nowInSeconds());
        Assert.assertEquals(Collections.singletonList(tombstone), categorized.expiredTombstones);
        Assert.assertEquals(Collections.singletonList(data), categorized.data);
        Assert.assertEquals(Collections.emptyList(), categorized.nearExpiryTombstones);
        Assert.assertEquals(Collections.emptyList(), categorized.latestTombstones);

        // 1 minute window
        assertEquals(10_000, TimeWindow.merge(data.timeWindowMs, tombstone.timeWindowMs).getDuration());

        TimeOrderedKeyCompactionStrategy.SSTablesForCompaction candidate = compactionStrategy.getNextBackgroundSSTables(FBUtilities.nowInSeconds());
        Assert.assertEquals(cfs.getLiveSSTables(), new HashSet<>(candidate.sstables));

        double garbagePercent = TimeOrderedKeyCompactionStrategy.getEstimatedGarbage(categorized.stats) * 100.0 / categorized.stats.onDiskLength;

        assertTrue(garbagePercent >= minGarbagePct);
        assertTrue(garbagePercent <= maxGarbagePct);
    }

    private List<SSTableReader> sort(Set<SSTableReader> sstables)
    {
        return sstables.stream().sorted(Comparator.comparingLong(o -> o.getSSTableMetadata().maxTimestamp)).collect(Collectors.toList());
    }

    private void populateData(ColumnFamilyStore cfs, int partitions, int rowsPerPartition, int rowDeletesPerPartition,
                              IntStream rowDeletePartitionRange, IntStream partitionDeleteRange)
    {
        for (int i = 0; i < partitions; ++i)
        {
            for (int j = 0; j < rowsPerPartition; ++j)
            {
                TOKCSUtil.insertStandard1(cfs, i, i, j, RandomStringUtils.randomAlphanumeric(4096));
            }
        }

        cfs.forceBlockingFlush();

        rowDeletePartitionRange.forEach(i -> {
            for (int j = 0; j < rowDeletesPerPartition; ++j)
            {
                // deleting a row with +10 time offset
                TOKCSUtil.deleteStandard1(cfs, i + 10, i, j);
            }
        });

        partitionDeleteRange.forEach(i -> {
            TOKCSUtil.deleteStandard1ByPartitionKey(cfs, i + 10, i);
        });

        cfs.forceBlockingFlush();
    }


    @Test
    public void testPrepWindows()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        // create 10 sstables over 2 window
        for (int s = 0; s < 10; ++s)
        {
            for (int i = 10 * s; i < 10 * s + 10; ++i)
            {
                TOKCSUtil.insertStandard1(cfs, i + 10);
                TOKCSUtil.deleteStandard1(cfs, i);
            }
            cfs.forceBlockingFlush();
        }

        assertEquals(20, cfs.getLiveSSTables().size());

        TimeOrderedKeyCompactionStrategy compactionStrategy = new TimeOrderedKeyCompactionStrategy(cfs, TOKCSUtil.getDefaultTOKCSOptions());
        cfs.getLiveSSTables().forEach(compactionStrategy::addSSTable);

        Set<SSTableReader> dataOnly = Util.dataOnly(cfs.getLiveSSTables());
        Set<SSTableReader> tombstoneOnly = Util.tombstonesOnly(cfs.getLiveSSTables());

        SSTableReader loneTombstone = tombstoneOnly.stream().min(Comparator.comparingLong(s -> s.getSSTableMetadata().minKey)).get();
        SSTableReader loneData = dataOnly.stream().max(Comparator.comparingLong(s -> s.getSSTableMetadata().maxKey)).get();

        // Case 1: with gcBefore = NO, no tombstone file is expired.
        TimeOrderedKeyCompactionStrategy.SSTablesForCompaction candidate = compactionStrategy.getNextBackgroundSSTables(CompactionManager.NO_GC);
        assertSame(TimeOrderedKeyCompactionStrategy.SSTablesForCompaction.EMPTY, candidate);

        // Case 2: with gcBefore to ALL, window1 should be chosen as it has most garbage

        Set<SSTableReader> chosenSStables = new HashSet<>();

        while((candidate = compactionStrategy.getNextBackgroundSSTables(CompactionManager.GC_ALL)) != TimeOrderedKeyCompactionStrategy.SSTablesForCompaction.EMPTY)
        {
            if(candidate.sstables.size() == 1) {
                assertEquals(loneTombstone, candidate.sstables.get(0));
            }
            else if(candidate.sstables.size() == 2) {
                assertEquals(1, Util.dataOnly(candidate.sstables).size());
                assertEquals(1, Util.tombstonesOnly(candidate.sstables).size());
            }
            else {
                fail("only expecting 2 or 1 candidate sstables for compaction.");
            }

            assertFalse(candidate.tombstoneMerge);
            assertTrue(candidate.splitSSTable);
            chosenSStables.addAll(candidate.sstables);
            // effect of compaction.
            candidate.sstables.forEach(compactionStrategy::removeSSTable);
        }

        Set<SSTableReader> expected = new HashSet<>();
        expected.addAll(dataOnly);
        expected.addAll(tombstoneOnly);
        expected.remove(loneData);

        assertEquals(expected, chosenSStables);
    }
}
