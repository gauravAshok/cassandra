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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class LongTimeOrderedKeyCompactionsTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongTimeOrderedKeyCompactionsTest.class);
    public static final String KEYSPACE1 = "Keyspace1";
    public static final String CF_STANDARD = "Standard1";
    private static final Random rndm = new Random(42);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // 1000,50.0 means compact when (1000 MB or 50% whichever is lower) of the data is garbage.
        Map<String, String> compactionOptions = TOKCSUtil.getTOKCSOptions("1000,30.0", "1000,30.0", 60, 8, 1000);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.TimeOrderedKeyCFMD.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                                   .compaction(
                                                                           CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, compactionOptions))
                                                                   .gcGraceSeconds(0));
    }

    @Before
    public void cleanupFiles()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
    }

    /**
     * Test compaction with a very wide row.
     */
    @Test
    public void testCompactionWide() throws Exception
    {
        // 60% deletion should trigger the compaction
        generateSStables(2, 200000, 60);

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        waitForCompaction(store);
        Assert.assertEquals(1, store.getLiveSSTables().size());
    }

    @Test
    public void testCompactionSlimNoCompact() throws Exception
    {
        // 25% deletion should not trigger the compaction.
        // There will be many sstables but there shouldn't be any overlap to justify the compaciton.

        // 40 partitions over 40 seconds. each partition roughly 40MB so a partition should not span over more than 2 sstables.
        generateSStables(40, 10000, 25);

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        Set<SSTableReader> beforeSSTables = store.getLiveSSTables();
        waitForCompaction(store);
        Assert.assertEquals(beforeSSTables, store.getLiveSSTables());
    }

    /**
     * Test compaction with lots of skinny rows.
     */
    @Test
    public void testCompactionSlim() throws Exception
    {
        generateSStables(400000, 1, 60);

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        Set<SSTableReader> dataforeCompaction = Util.dataOnly(store.getLiveSSTables());
        waitForCompaction(store);

        Set<SSTableReader> sstables = store.getLiveSSTables();
        Assert.assertEquals(0, Util.tombstonesOnly(sstables).size());
        logger.info("total files : {}", sstables.size());
        // since the total size is actually around 1.6GB, our 1gb threshold will force the tombstone to split by split factor.
        // since there will be 8 splits. further compaction can also result in further 8 splits.
        // Hence putting a upper bound on 64 + 8 (for some room)
        Assert.assertTrue(sstables.size() < (64 + 8));
    }

    private void waitForCompaction(ColumnFamilyStore store)
    {
        store.enableAutoCompaction(true);
        CompactionManager.instance.waitForCessation(Arrays.asList(store), sstable -> true);
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        while (true)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            if(!CompactionManager.instance.isCompacting(Arrays.asList(store), sstable -> true))
            {
                return;
            }
        }
    }

    protected void generateSStables(int paritions, int rowsPerPartition, int deletePercent) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        store.disableAutoCompaction();

        for (int j = 0; j < paritions; j++)
        {
            // last sstable has highest timestamps
            for (int i = 0; i < rowsPerPartition; i++)
            {
                TOKCSUtil.insertStandard1(store, FBUtilities.nowInSeconds(), j, i, i + " - " + RandomStringUtils.randomAlphanumeric(4096));
            }
        }

        // deletes
        for (int j = 0; j < paritions; j++)
        {
            // last sstable has highest timestamps
            for (int i = 0; i < rowsPerPartition; i++)
            {
                if (rndm.nextInt(100) < deletePercent)
                {
                    TOKCSUtil.deleteStandard1(store, FBUtilities.nowInSeconds(), j, i);
                }
            }
        }
        store.forceBlockingFlush();
    }

    @Test
    public void testStandardColumnCompactions()
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.clearUnsafe();

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata().params.minIndexInterval * 3 / ROWS_PER_SSTABLE;

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpectedSec = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<>();
        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                long timestampSec = j * ROWS_PER_SSTABLE + i;
                DecoratedKey key = TOKCSUtil.insertStandard1(cfs, timestampSec, i % 2, i / 2, RandomStringUtils.randomAlphanumeric(4096));
                maxTimestampExpectedSec = Math.max(timestampSec, maxTimestampExpectedSec);
                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpectedSec * 1000000L);

            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        forceCompactions(cfs);
        assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpectedSec * 1000000L);
        cfs.truncateBlocking();
    }

    private void forceCompactions(ColumnFamilyStore cfs)
    {
        // re-enable compaction with thresholds low enough to force a few rounds
        cfs.setCompactionThresholds(2, 4);
        waitForCompaction(cfs);
    }
}
