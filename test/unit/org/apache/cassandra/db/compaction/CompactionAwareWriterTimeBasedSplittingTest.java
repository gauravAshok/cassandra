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

import com.google.common.primitives.Longs;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.writers.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class CompactionAwareWriterTimeBasedSplittingTest extends CQLTester
{
    private static final String KEYSPACE = "cawt_keyspace";
    private static final String TABLE = "cawt_table";

    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE %s.%s (k TIMESTAMP, name TEXT, k_ck TIMESTAMP, t int, v blob, PRIMARY KEY ((k, name), k_ck, t)) \n" +
                TOKCSUtil.getCQLFramgentForTOKCS("1024,50", 0), KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDownClass()
    {
        QueryProcessor.executeInternal("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private ColumnFamilyStore getColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @Test
    public void testSplittingSizeTieredCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rowCount = 5000;
        populate(rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        CompactionAwareWriter writer =
                new TimeBasedSplittingCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals(), false, cfs.metadata.partitionKeyColumns().size() > 1,
                        60, 0, rowCount / 60 + 1, 0);

        int rows = compact(cfs, txn, writer);
        long expectedSize = beforeSize * 60 / rowCount;
        List<SSTableReader> sortedSSTables = new ArrayList<>(cfs.getLiveSSTables());

        Collections.sort(sortedSSTables, (o1, o2) -> Longs.compare(o2.onDiskLength(), o1.onDiskLength()));

        for(int i = 0; i < sortedSSTables.size() - 1; ++i) {
            // allow 1% diff in estimated vs actual size
            Assert.assertEquals((double)expectedSize, sortedSSTables.get(i).onDiskLength(), expectedSize / 100.0);
        }

        long lastFileExpectedSize = beforeSize * (rowCount % 60) / rowCount;
        // allow 10% diff in estimated vs actual size
        Assert.assertEquals((double)lastFileExpectedSize, sortedSSTables.get(sortedSSTables.size() - 1).onDiskLength(), lastFileExpectedSize / 10.0);

        Assert.assertEquals(rowCount, rows);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    private int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
    {
        assert txn.originals().size() == 1;
        int rowsWritten = 0;
        int nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID()))
        {
            while (ci.hasNext())
            {
                if (writer.append(ci.next()))
                    rowsWritten++;
            }
        }
        writer.finish();
        return rowsWritten;
    }

    private void populate(int count) throws Throwable
    {
        byte [] payload = new byte[5000];
        new Random(42).nextBytes(payload);
        ByteBuffer b = ByteBuffer.wrap(payload);

        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < ROW_PER_PARTITION; j++)
            {
                Date key = Util.dt(i);
                execute(String.format("INSERT INTO %s.%s(k, name, k_ck, t, v) VALUES (?, ?, ?, ?, ?)", KEYSPACE, TABLE), key, "name", key, j, b);
            }
        }

        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.forceBlockingFlush();
        assert cfs.getLiveSSTables().size() == 1 : cfs.getLiveSSTables();
    }

    private void validateData(ColumnFamilyStore cfs, int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Date key = Util.dt(i);
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(key, "name", key, j);

            assertRows(execute(String.format("SELECT k, name, k_ck, t FROM %s.%s WHERE k = ? and name = ?", KEYSPACE, TABLE), key, "name"), expected);
        }
    }
}
