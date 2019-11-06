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
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
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
        Map<String, String> compactionOptions = TOKCSUtil.getTOKCSOptions("1000,50.0", "1000,50.0", 30, 3);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                KeyspaceParams.simple(1),
                SchemaLoader.TimeOrderedKeyCFMD.standardCFMD(KEYSPACE1, CF_STANDARD)
                        .compaction(CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, compactionOptions))
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

        store.enableAutoCompaction(true);
        Assert.assertEquals(0, store.getCompactionStrategyManager().getEstimatedRemainingTasks());
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
        int beforeSStables = store.getLiveSSTables().size();

        store.enableAutoCompaction(true);
        Assert.assertEquals(1, store.getCompactionStrategyManager().getEstimatedRemainingTasks());
        Assert.assertEquals(beforeSStables, store.getLiveSSTables().size());
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
        store.enableAutoCompaction(true);

        Thread.sleep(30000);

        logger.info("pending tasks: {}", store.getCompactionStrategyManager().getEstimatedRemainingTasks());
        logger.info("files after compaction: {}", store.getLiveSSTables().size());
    }

    /**
     * Test compaction with lots of small sstables.
     */
    @Test
    public void testCompactionMany() throws Exception
    {
        generateSStables(80000, 5, 60);
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
                TOKCSUtil.insertStandard1(store, FBUtilities.nowInSeconds(), j, i, i + " - " + RandomStringUtils.randomAlphanumeric(4096));
        }

        // deletes
        for (int j = 0; j < paritions; j++)
        {
            // last sstable has highest timestamps
            for (int i = 0; i < rowsPerPartition; i++)
            {
                if(rndm.nextInt(100) < deletePercent) {
                    TOKCSUtil.deleteStandard1(store, FBUtilities.nowInSeconds(), j, i);
                }
            }
        }
        store.forceBlockingFlush();
    }

    void logcfsState(ColumnFamilyStore store) {
        Set<SSTableReader> sstables = store.getLiveSSTables();
        Set<SSTableReader> data = Util.dataOnly(sstables);
        Set<SSTableReader> tombstone = Util.tombstonesOnly(sstables);
        logger.info("sstables={}[d:{},t:{}]", sstables.size(), data.size(), tombstone.size());
    }

    @Test
    public void testStandardColumnCompactions()
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.clearUnsafe();

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata.params.minIndexInterval * 3 / ROWS_PER_SSTABLE;

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                long timestamp = j * ROWS_PER_SSTABLE + i;
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                UpdateBuilder.create(cfs.metadata, key)
                        .withTimestamp(timestamp)
                        .newRow(String.valueOf(i / 2)).add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .apply();

                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);

            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        forceCompactions(cfs);
        assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
        cfs.truncateBlocking();
    }

    private void forceCompactions(ColumnFamilyStore cfs)
    {
        // re-enable compaction with thresholds low enough to force a few rounds
        cfs.setCompactionThresholds(2, 4);

        // loop submitting parallel compactions until they all return 0
        do
        {
            ArrayList<Future<?>> compactions = new ArrayList<Future<?>>();
            for (int i = 0; i < 10; i++)
            {
                compactions.addAll(CompactionManager.instance.submitBackground(cfs));
            }
            // another compaction attempt will be launched in the background by
            // each completing compaction: not much we can do to control them here
            FBUtilities.waitOnFutures(compactions);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        if (cfs.getLiveSSTables().size() > 1)
        {
            CompactionManager.instance.performMaximal(cfs, false);
        }
    }
}
