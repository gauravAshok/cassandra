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

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AbstractCompactionStrategyTimeOrderedKeyTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String TOKCS_TABLE = "TOKCS_TABLE";

    @BeforeClass
    public static void loadData() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.TimeOrderedKeyCFMD.standardCFMD(KEYSPACE1, TOKCS_TABLE)
                                                .compaction(CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, getDefaultTOKCSOptions())));
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TOKCS_TABLE).disableAutoCompaction();
    }

    @After
    public void tearDown()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TOKCS_TABLE).truncateBlocking();
    }

    @Test(timeout=90000)
    public void testGetNextBackgroundTaskDoesNotBlockTOKCSTombstoneRange()
    {
        testGetNextBackgroundTaskDoesNotBlock(TOKCS_TABLE, true);
    }

    @Test(timeout=90000)
    public void testGetNextBackgroundTaskDoesNotBlockTOKCSNormalTombstone()
    {
        testGetNextBackgroundTaskDoesNotBlock(TOKCS_TABLE, false);
    }

    public void testGetNextBackgroundTaskDoesNotBlock(String table, boolean tombstoneRange)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);

        Util.waitUptoNearestSeconds(10);

        // Add 4 sstables
        for (int i = 1; i <= 4; i++)
        {
            insertKeyAndFlush(table, Util.dt(i));
        }

        // and then delete everything
        for (int i = 1; i <= 4; i++)
        {
            if(tombstoneRange)
            {
                deleteKeyByWithTombstoneRangeAndFlush(table, Util.dt(i));
            }
            else
            {
                deleteKeyAndFlush(table, Util.dt(i));
            }
        }

        Util.waitUptoNearestSeconds(60); // wait for a minute, because TOKCS only considers window that has been completed

        // Check they are returned on the next background task
        try (LifecycleTransaction txn = strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()).transaction)
        {
            Assert.assertEquals(cfs.getLiveSSTables(), txn.originals());
        }

        // now remove sstables on the tracker, to simulate a concurrent transaction
        cfs.getTracker().removeUnsafe(cfs.getLiveSSTables());

        // verify the compaction strategy will return null
        Assert.assertNull(strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()));
    }

    private static void insertKeyAndFlush(String table, Date key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk(ByteBufferUtil.bytes(key.getTime()));
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);
        new RowUpdateBuilder(cfs.metadata, timestamp, dk.getKey())
        .clustering(key, String.valueOf(key))
        .add("val", "val_" + key)
        .add("val0", "val0_" + key)
        .build()
        .applyUnsafe();
        cfs.forceBlockingFlush();
    }

    private static void deleteKeyByWithTombstoneRangeAndFlush(String table, Date key)
    {
        long timestamp = System.currentTimeMillis();
        int timeInSec = FBUtilities.nowInSeconds();
        DecoratedKey dk = Util.dk(ByteBufferUtil.bytes(key.getTime()));
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);

        RowUpdateBuilder deletedRowUpdateBuilder = new RowUpdateBuilder(cfs.metadata, timestamp, dk.getKey());

        ByteBuffer[] buffers = {ByteBufferUtil.bytes(key.getTime()), ByteBufferUtil.bytes(String.valueOf(key))};

        Clustering startClustering = Clustering.make(buffers);
        Clustering endClustering = Clustering.make(buffers);
        deletedRowUpdateBuilder.addRangeTombstone(new RangeTombstone(Slice.make(startClustering, endClustering), new DeletionTime(timestamp, timeInSec)));
        deletedRowUpdateBuilder.build().applyUnsafe();

        cfs.forceBlockingFlush();
    }

    private static void deleteKeyAndFlush(String table, Date key)
    {
        long timestamp = System.currentTimeMillis();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);
        RowUpdateBuilder.deleteRow(cfs.metadata, timestamp, key, key, String.valueOf(key)).applyUnsafe();

        cfs.forceBlockingFlush();
    }

    protected static Map<String, String> getDefaultTOKCSOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, "128,50");
        options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, "1024,50");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        return options;
    }
}
