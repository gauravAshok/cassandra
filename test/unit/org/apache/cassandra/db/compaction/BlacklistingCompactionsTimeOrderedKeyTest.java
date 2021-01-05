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
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.util.*;

import static org.junit.Assert.*;

public class BlacklistingCompactionsTimeOrderedKeyTest
{
    private static final Logger logger = LoggerFactory.getLogger(BlacklistingCompactionsTimeOrderedKeyTest.class);

    private static Random random;

    private static final String KEYSPACE1 = "BlacklistingCompactionsTest";
    private static final String STANDARD_TOKCS = "Standard_TOKCS";
    private static int maxValueSize;

    @After
    public void leakDetect() throws InterruptedException
    {
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(10);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        long seed = System.nanoTime();
        //long seed = 754271160974509L; // CASSANDRA-9530: use this seed to reproduce compaction failures if reading empty rows
        //long seed = 2080431860597L; // CASSANDRA-12359: use this seed to reproduce undetected corruptions
        logger.info("Seed {}", seed);
        random = new Random(seed);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    makeTable(STANDARD_TOKCS)
                                            .compaction(CompactionParams.create(TimeOrderedKeyCompactionStrategy.class, TOKCSUtil.getDefaultTOKCSOptions()))
                                            .timeOrderedKey(true));

        maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024);
        closeStdErr();
    }

    /**
     * Return a table metadata, we use types with fixed size to increase the chance of detecting corrupt data
     */
    private static TableMetadata.Builder makeTable(String tableName)
    {
        return SchemaLoader.TimeOrderedKeyCFMD.standardCFMD(KEYSPACE1, tableName, 1, LongType.instance, LongType.instance);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);
    }

    public static void closeStdErr()
    {
        // These tests generate an error message per CorruptSSTableException since it goes through
        // DebuggableThreadPoolExecutor, which will log it in afterExecute.  We could stop that by
        // creating custom CompactionStrategy and CompactionTask classes, but that's kind of a
        // ridiculous amount of effort, especially since those aren't really intended to be wrapped
        // like that.
        System.err.close();
    }

    @Test
    public void testBlacklistingWithTimeOrderedKeyCompactionStrategy() throws Exception
    {
        testBlacklisting(STANDARD_TOKCS);
    }

    private void testBlacklisting(String tableName) throws Exception
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata().params.minIndexInterval * 2 / ROWS_PER_SSTABLE;
        final int SSTABLES_TO_CORRUPT = 8;

        assertTrue(String.format("Not enough sstables (%d), expected at least %d sstables to corrupt", SSTABLES, SSTABLES_TO_CORRUPT),
                   SSTABLES > SSTABLES_TO_CORRUPT);

        // disable compaction while flushing
        cfs.disableAutoCompaction();
        //test index corruption
        //now create a few new SSTables
        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<>();

        for (long j = 0; j < SSTABLES; j++)
        {
            for (long i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                long timestamp = j * ROWS_PER_SSTABLE + i;
                Date key = Util.dt(timestamp);
                DecoratedKey dk = Util.dk(key.getTime(), 1000);
                new RowUpdateBuilder(cfs.metadata(), timestamp, dk)
                        .clustering(i)
                        .add("val0", i)
                        .build()
                        .applyUnsafe();
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                inserted.add(dk);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        int currentSSTable = 0;

        // corrupt first 'sstablesToCorrupt' SSTables
        for (SSTableReader sstable : sstables)
        {
            if (currentSSTable + 1 > SSTABLES_TO_CORRUPT)
                break;

            RandomAccessFile raf = null;

            try
            {
                int corruptionSize = 100;
                raf = new RandomAccessFile(sstable.getFilename(), "rw");
                assertNotNull(raf);
                assertTrue(raf.length() > corruptionSize);
                long pos = random.nextInt((int)(raf.length() - corruptionSize));
                logger.info("Corrupting sstable {} [{}] at pos {} / {}", currentSSTable, sstable.getFilename(), pos, raf.length());
                raf.seek(pos);
                // We want to write something large enough that the corruption cannot get undetected
                // (even without compression)
                byte[] corruption = new byte[corruptionSize];
                random.nextBytes(corruption);
                raf.write(corruption);
                if (ChunkCache.instance != null)
                    ChunkCache.instance.invalidateFile(sstable.getFilename());

            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }

            currentSSTable++;
        }

        int failures = 0;

        // in case something will go wrong we don't want to loop forever using for (;;)
        for (int i = 0; i < sstables.size(); i++)
        {
            try
            {
                cfs.forceMajorCompaction();
            }
            catch (Exception e)
            {
                // kind of a hack since we're not specifying just CorruptSSTableExceptions, or (what we actually expect)
                // an ExecutionException wrapping a CSSTE.  This is probably Good Enough though, since if there are
                // other errors in compaction presumably the other tests would bring that to light.
                failures++;
                continue;
            }
            break;
        }

        cfs.truncateBlocking();
        assertEquals(SSTABLES_TO_CORRUPT, failures);
    }
}
