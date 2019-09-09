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

import com.google.common.collect.Iterables;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
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
                        .timeOrderedKey(true));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY, "1");
        options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, "MINUTES");
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
            options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY, "1");
        }

        try
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY, "-1");
            validateOptions(options);
            fail(String.format("%s == -1 should be rejected", TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_KEY, "1");
        }

        String[] validUnits = {"MINUTES", "HOURS", "DAYS"};
        String[] invalidUnits = {"SECONDS", "MONTHS", "YEARS", "RANDOM"};

        for (String valid : validUnits)
        {
            try
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, valid);
                validateOptions(options);
            }
            catch (Exception e)
            {
                fail(String.format("%s == %s should not be rejected", TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, valid));
            }
        }

        for (String invalid : invalidUnits)
        {
            try
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, invalid);
                validateOptions(options);
                fail(String.format("%s == %s should be rejected", TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, invalid));
            }
            catch (ConfigurationException e)
            {
                options.put(TimeOrderedKeyCompactionStrategyOptions.TOMBSTONE_COMPACTION_DELAY_UNIT_KEY, "MINUTES");
            }
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

        Assert.assertEquals(pair(epoch("2019-08-09 13:01:00"), 1), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts2, 60));
        Assert.assertEquals(pair(epoch("2019-08-09 13:01:00"), 3), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts3, 30));
        Assert.assertEquals(pair(epoch("2019-08-09 13:01:00"), 8), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts4, 15));
        Assert.assertEquals(pair(epoch("2019-08-09 13:01:00"), 120), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts5, 30));
        Assert.assertEquals(pair(epoch("2019-08-09 13:01:00"), 59 + 60 * 5), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts6, 60));
        Assert.assertEquals(pair(epoch("2019-08-09 13:00:00"), 60 * 11 / 2), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts7, 120));
        Assert.assertEquals(pair(epoch("2019-08-09 13:00:00"), 60 * 24 / 4 + 1), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts8, 240));
        Assert.assertEquals(pair(epoch("2019-08-09 12:56:00"), 60 * 24 / 8 + 1), TimeOrderedKeyCompactionStrategy.rangeToWindow(ts1, ts9, 480));
    }

    private long epoch(String str)
    {
        return LocalDateTime.parse(str, f).toEpochSecond(ZoneOffset.UTC);
    }

    private Pair<Long, Long> pair(long v1, long v2)
    {
        return Pair.create(v1, v2);
    }


    @Test
    public void testMetadata() {
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

        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        List<SSTableReader> files = sort(cfs.getLiveSSTables());
        SSTableReader data = files.get(0), tombstone = files.get(1);

        // get the compaction task
        TimeOrderedKeyCompactionStrategy compactionStrategy = new TimeOrderedKeyCompactionStrategy(cfs, TOKCSUtil.getDefaultTOKCSOptions());

        List<SSTableReader> expired = compactionStrategy.getFullyExpiredSStables(cfs.getLiveSSTables(), FBUtilities.nowInSeconds());
        Assert.assertEquals(Collections.singletonList(tombstone), expired);

        TimeOrderedKeyCompactionStrategy.SSTablesStats stats = compactionStrategy.buildPerWindowSStablesStats(cfs, expired, 60);
        compactionStrategy.populateGlobalStats(cfs, stats);

        // 1 window
        assertEquals(1, stats.windowedStats.size());

        // sstables
        TimeOrderedKeyCompactionStrategy.WindowedSStablesStats windowStats = stats.windowedStats.get(0L);
        assertEquals(Collections.singletonList(tombstone), windowStats.tombstoneSStables);
        assertEquals(Collections.singletonList(data), windowStats.dataSStables);

        double garbagePercent = (double)windowStats.estimatedGarbage * 100.0 / windowStats.dataSizeOnDisk;

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
        Set<SSTableReader> tombstones = Util.tombstonesOnly(cfs.getLiveSSTables());

        TimeOrderedKeyCompactionStrategy compactionStrategy = new TimeOrderedKeyCompactionStrategy(cfs, TOKCSUtil.getDefaultTOKCSOptions());

        // Case 1: with gcBefore = NO, no tombstone file is expired.
        TimeOrderedKeyCompactionStrategy.SSTablesForCompaction candidate = compactionStrategy.getNextBackgroundSSTables(CompactionManager.NO_GC);

        // expect the tombstone only compactions
        assertSame(TimeOrderedKeyCompactionStrategy.SSTablesForCompaction.EMPTY, candidate);

        // Case 2: with gcBefore to ALL, window1 should be chosen as it has most garbage
        candidate = compactionStrategy.getSSTablesForCompaction(CompactionManager.GC_ALL, new ArrayList<>(tombstones));
        Set<SSTableReader> expectedWindow1 = cfs.getLiveSSTables().stream().filter(s -> s.getSSTableMetadata().minKey < 60000).collect(Collectors.toSet());

        assertEquals(expectedWindow1, new HashSet<>(candidate.sstables));
        assertFalse(candidate.tombstoneMerge);
        assertTrue(candidate.splitSStable);

        Set<SSTableReader> remainingTombstones = new HashSet<>(tombstones);
        remainingTombstones.removeAll(candidate.sstables);

        // Case 3: check for remaining sstable now.
        candidate = compactionStrategy.getSSTablesForCompaction(CompactionManager.GC_ALL, new ArrayList<>(remainingTombstones));

        // last data sstable has the data that is not deleted
        SSTableReader lastDataSSTable = Util.dataOnly(cfs.getLiveSSTables()).stream().max(Comparator.comparingLong(s -> s.maxDataAge)).get();
        Set<SSTableReader> expectedWindow2 = new HashSet<>(cfs.getLiveSSTables());
        expectedWindow2.removeAll(expectedWindow1);
        expectedWindow2.remove(lastDataSSTable);

        assertEquals(expectedWindow2, new HashSet<>(candidate.sstables));
        assertFalse(candidate.tombstoneMerge);
        assertTrue(candidate.splitSStable);
    }
}
