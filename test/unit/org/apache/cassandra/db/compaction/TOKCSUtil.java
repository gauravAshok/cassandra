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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TOKCSUtil
{
    public static void insertStandard1(ColumnFamilyStore cfs, int i) {
        Date key = Util.dt(i);
        ByteBuffer keyBuf = ByteBufferUtil.bytes(key.getTime());
        new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), keyBuf)
                .clustering(key, String.valueOf(key))
                .add("val", "val_" + key)
                .add("val0", "val0_" + key)
                .build()
                .applyUnsafe();
    }

    public static void deleteStandard1(ColumnFamilyStore cfs, int i) {
        Date key = Util.dt(i);
        long timestamp = System.currentTimeMillis();
        RowUpdateBuilder.deleteRow(cfs.metadata, timestamp, key, key, String.valueOf(key)).applyUnsafe();
    }

    public static void deleteRangeStandard1(ColumnFamilyStore cfs, int i) {
        long timestamp = System.currentTimeMillis();
        int timeInSec = FBUtilities.nowInSeconds();
        Date key = Util.dt(i);

        RowUpdateBuilder deletedRowUpdateBuilder = new RowUpdateBuilder(cfs.metadata, timestamp, ByteBufferUtil.bytes(key.getTime()));

        ByteBuffer[] buffers = {
                ByteBufferUtil.bytes(key.getTime()),
                ByteBufferUtil.bytes(String.valueOf(key))
        };

        Clustering startClustering = Clustering.make(buffers);
        Clustering endClustering = Clustering.make(buffers);
        deletedRowUpdateBuilder.addRangeTombstone(new RangeTombstone(Slice.make(startClustering, endClustering), new DeletionTime(timestamp, timeInSec)));
        deletedRowUpdateBuilder.build().applyUnsafe();

        cfs.forceBlockingFlush();
    }

    public static Map<String, String> getDefaultTOKCSOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(TimeOrderedKeyCompactionStrategyOptions.WINDOW_GARBAGE_SIZE_THRESHOLD_KEY, "128,50");
        options.put(TimeOrderedKeyCompactionStrategyOptions.GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY, "1024,50");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        return options;
    }

    public static String getCQLFramgentForTOKCS(String threshold, int gcGraceSec) {
        return String.format("WITH compaction = {" +
                "'class':'org.apache.cassandra.db.compaction.TimeOrderedKeyCompactionStrategy', " +
                "'compaction_window_unit':'MINUTES', " +
                "'compaction_window_size':'1', " +
                "'window_garbage_size_threshold':'%s', " +
                "'global_garbage_size_threshold':'%s'" +
                "} " +
                "AND time_ordered_ck = true " +
                "AND gc_grace_seconds = %d", threshold, threshold, gcGraceSec);
    }
}
