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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

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
                                                                   .compaction(CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, TOKCSUtil.getTOKCSOptions("0,0", "0,0", 1, 3, 10000)))
                                                                   .gcGraceSeconds(0));
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TOKCS_TABLE).disableAutoCompaction();
    }

    @After
    public void tearDown()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TOKCS_TABLE).truncateBlocking();
    }

    @Test(timeout = 90000)
    public void testGetNextBackgroundTaskDoesNotBlockTOKCSTombstoneRange()
    {
        testGetNextBackgroundTaskDoesNotBlock(TOKCS_TABLE, true);
    }

    @Test(timeout = 90000)
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
        IntStream.range(1, 5).forEach(i -> {
            TOKCSUtil.insertStandard1(cfs, i);
            cfs.forceBlockingFlush();
        });

        IntConsumer deleter = tombstoneRange
                ? i -> TOKCSUtil.deleteRangeStandard1(cfs, i)
                : i -> TOKCSUtil.deleteStandard1(cfs, i);

        // and then delete everything
        IntStream.range(1, 5).forEach(i -> {
            deleter.accept(i);
            cfs.forceBlockingFlush();
        });

        Set<SSTableReader> beforeCompaction = cfs.getLiveSSTables();
        Set<SSTableReader> chosenForCompaction = new HashSet<>();

        // Check they are returned on the next background task
        int compactionRound = 4;
        while (--compactionRound >= 0)
        {
            // pass now() + 1 as gc_before, because latest tombstones will obviously not be expired this soon.
            try (LifecycleTransaction txn = strategy.getNextBackgroundTask(FBUtilities.nowInSeconds() + 1).transaction)
            {
                Assert.assertEquals(compactionRound + 1, strategy.getEstimatedRemainingTasks());
                Assert.assertEquals(2, txn.originals().size());
                Set<SSTableReader> dataOnly = Util.dataOnly(txn.originals());
                Set<SSTableReader> tombstoneOnly = Util.tombstonesOnly(txn.originals());

                Assert.assertEquals(1, dataOnly.size());
                Assert.assertEquals(1, tombstoneOnly.size());

                chosenForCompaction.addAll(txn.originals());
                // now remove sstables on the tracker, to simulate a concurrent transaction
                cfs.getTracker().removeUnsafe(txn.originals());
            }
        }

        Assert.assertEquals(beforeCompaction, chosenForCompaction);

        // verify the compaction strategy will return null
        Assert.assertNull(strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()));
    }
}
