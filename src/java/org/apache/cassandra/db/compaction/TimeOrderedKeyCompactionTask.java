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

import com.google.common.collect.HashMultimap;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.TimeBasedSplittingCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ComparablePair;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TimeOrderedKeyCompactionTask extends CompactionTask
{

    private final long windowSizeInMin;
    private final TimeWindowCompactionStrategyOptions twcsOptions;

    public TimeOrderedKeyCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, TimeWindowCompactionStrategyOptions twcsOptions) {

        super(cfs, txn, gcBefore);
        this.twcsOptions = twcsOptions;
        this.windowSizeInMin = TimeUnit.MINUTES.convert(twcsOptions.sstableWindowSize, twcsOptions.sstableWindowUnit);
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(
            ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {

        HashMultimap<ComparablePair<Integer, Long>, SSTableReader> buckets =
            TimeOrderedKeyCompactionStrategy.getBuckets(
                    nonExpiredSSTables, twcsOptions.sstableWindowUnit, twcsOptions.sstableWindowSize, twcsOptions.timestampResolution)
                .left;

        long windowStart = Long.MAX_VALUE;
        long windowEnd = 0;
        for (ComparablePair<Integer, Long> key : buckets.keySet()) {
            windowStart = Math.min(windowStart, key.right);

            long end = key.right + windowSizeInMin * key.left;
            windowEnd = Math.max(windowEnd, end);
        }

        return new TimeBasedSplittingCompactionWriter(
            cfs, directories, txn, nonExpiredSSTables, false, (int) windowSizeInMin, windowStart, (int) ((windowEnd - windowStart) / windowSizeInMin));
    }
}
