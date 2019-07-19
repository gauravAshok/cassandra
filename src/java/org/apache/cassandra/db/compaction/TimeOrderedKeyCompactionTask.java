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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.TimeBasedSplittingCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TimeOrderedKeyCompactionTask extends CompactionTask
{
    private final TimeWindowCompactionStrategyOptions twcsOptions;
    private final long windowSizeInSec;
    private final boolean splitExpected;
    private final boolean tombstoneOnlyMerge;

    public TimeOrderedKeyCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, TimeWindowCompactionStrategyOptions twcsOptions, boolean splitExpected, boolean tombstoneOnlyMerge)
    {

        super(cfs, txn, gcBefore);
        this.twcsOptions = twcsOptions;
        this.windowSizeInSec = TimeUnit.SECONDS.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
        this.splitExpected = splitExpected;
        this.tombstoneOnlyMerge = tombstoneOnlyMerge;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(
            ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
    {

        long windowStart = Long.MAX_VALUE;
        long windowEnd = 0;

        if (splitExpected)
        {
            for (SSTableReader sstable : transaction.originals())
            {
                Pair<Long, Long> window = TimeOrderedKeyCompactionStrategy.getWindow(sstable, windowSizeInSec);
                windowStart = Math.min(windowStart, window.left);

                long end = window.left + windowSizeInSec * window.right;
                windowEnd = Math.max(windowEnd, end);
            }

            return new TimeBasedSplittingCompactionWriter(
                    cfs, directories, txn, nonExpiredSSTables, false, windowSizeInSec, windowStart, (windowEnd - windowStart) / windowSizeInSec, getLevel());
        }

        // if split is not expected, we are just looking at normal compaction between sstables
        return super.getCompactionAwareWriter(cfs, directories, txn, nonExpiredSSTables);
    }

    @Override
    protected int getLevel()
    {
        return tombstoneOnlyMerge ? Memtable.TOMBSTONE_SSTABLE_LVL : Memtable.DATA_SSTABLE_LVL;
    }
}
