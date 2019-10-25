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

import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TOKCSUtil
{
    public static void insertStandard1(ColumnFamilyStore cfs, int i)
    {
        insertStandard1(cfs, FBUtilities.nowInSeconds(), i, i, null);
    }

    public static void insertStandard1(ColumnFamilyStore cfs, long timestampInSec, int i, int j, String payload)
    {
        Date key = Util.dt(i);
        payload = payload == null ? key.toString() : payload;
        new RowUpdateBuilder(cfs.metadata, timestampInSec * 1000000L, Util.dk(key.getTime(), 1000))
                .clustering(String.valueOf(j))
                .add("val0", "val0_" + payload)
                .build()
                .applyUnsafe();
    }

    public static void deleteStandard1(ColumnFamilyStore cfs, int i)
    {
        deleteStandard1(cfs, FBUtilities.nowInSeconds(), i, i);
    }

    public static void deleteStandard1(ColumnFamilyStore cfs, int timestampInSec, int i, int j)
    {
        DecoratedKey key = Util.dk(Util.dt(i).getTime(), 1000);
        RowUpdateBuilder.deleteRowAt(cfs.metadata, timestampInSec * 1000000L, timestampInSec, key, String.valueOf(j)).applyUnsafe();
    }

    public static void deleteStandard1ByPartitionKey(ColumnFamilyStore cfs, int i)
    {
        deleteStandard1ByPartitionKey(cfs, FBUtilities.nowInSeconds(), i);
    }

    public static void deleteStandard1ByPartitionKey(ColumnFamilyStore cfs, int timestampInSec, int i)
    {
        Date key = Util.dt(i);
        PartitionUpdate update = PartitionUpdate.fullPartitionDelete(cfs.metadata, Util.dk(key.getTime(), 1000), timestampInSec * 1000000L, timestampInSec);
        new Mutation(update.metadata().ksName, update.partitionKey()).add(update).applyUnsafe();
    }

    public static void deleteRangeStandard1(ColumnFamilyStore cfs, int i)
    {
        deleteRangeStandard1(cfs, FBUtilities.nowInSeconds(), i, i, i);
    }

    public static void deleteRangeStandard1(ColumnFamilyStore cfs, int timestampInSec, int i, int from, int to)
    {
        int timeInSec = FBUtilities.nowInSeconds();
        Date key = Util.dt(i);

        RowUpdateBuilder deletedRowUpdateBuilder = new RowUpdateBuilder(cfs.metadata, timestampInSec * 1000000L, Util.dk(key.getTime(), 1000));

        Clustering startClustering = Clustering.make(ByteBufferUtil.bytes(String.valueOf(from)));
        Clustering endClustering = Clustering.make(ByteBufferUtil.bytes(String.valueOf(to)));

        deletedRowUpdateBuilder.addRangeTombstone(new RangeTombstone(Slice.make(startClustering, endClustering), new DeletionTime(timestampInSec, timeInSec)));
        deletedRowUpdateBuilder.build().applyUnsafe();

        cfs.forceBlockingFlush();
    }

    public static Map<String, String> getDefaultTOKCSOptions()
    {
        return getTOKCSOptions("128,50", "1024,50", 1);
    }

    public static Map<String, String> getTOKCSOptions(String windowGarbageThreshold, String globalGarbageThreshold, int compactionWindowSize) {
        Map<String, String> options = new HashMap<>();
        options.put(TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, windowGarbageThreshold);
        options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, globalGarbageThreshold);
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, String.valueOf(compactionWindowSize));
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        return options;
    }

    public static String getCQLFramgentForTOKCS(String threshold, int gcGraceSec)
    {
        return String.format("WITH compaction = {" +
                "'class':'org.apache.cassandra.db.compaction.TimeOrderedKeyCompactionStrategy', " +
                "'compaction_window_unit':'MINUTES', " +
                "'compaction_window_size':'1', " +
                "'window_garbage_size_threshold':'%s', " +
                "'global_garbage_size_threshold':'%s'" +
                "} " +
                "AND time_ordered_key = true " +
                "AND gc_grace_seconds = %d", threshold, threshold, gcGraceSec);
    }
}
