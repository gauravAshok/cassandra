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

package org.apache.cassandra.db.lifecycle;

import com.google.common.collect.Iterables;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.commons.collections.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SSTableTimeIntervalTree extends IntervalTree<Long, SSTableReader, Interval<Long, SSTableReader>>
{
    private static final SSTableTimeIntervalTree EMPTY = new SSTableTimeIntervalTree(null);

    SSTableTimeIntervalTree(Collection<Interval<Long, SSTableReader>> intervals)
    {
        super(intervals);
    }

    public static SSTableTimeIntervalTree empty()
    {
        return EMPTY;
    }

    public static SSTableTimeIntervalTree build(Iterable<SSTableReader> sstables)
    {
        return new SSTableTimeIntervalTree(buildIntervalsBasedOnClustering(sstables));
    }

    public static List<Interval<Long, SSTableReader>> buildIntervalsBasedOnClustering(Iterable<SSTableReader> sstables)
    {
        List<Interval<Long, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
        {
            StatsMetadata metadata = sstable.getSSTableMetadata();
            Interval<Long, Void> bound = intervalFromClusteringKey(metadata);
            intervals.add(Interval.create(bound.min, bound.max, sstable));
        }
        return intervals;
    }

    static Interval<Long, Void> intervalFromClusteringKey(StatsMetadata metadata) {

        long lb = CollectionUtils.isEmpty(metadata.minClusteringValues) ? Long.MIN_VALUE : getLongFromBuffer(metadata.minClusteringValues.get(0));
        long ub = CollectionUtils.isEmpty(metadata.maxClusteringValues) ? Long.MAX_VALUE: getLongFromBuffer(metadata.maxClusteringValues.get(0));

        return Interval.create(lb, ub);
    }

    static long getLongFromBuffer(ByteBuffer buffer) {
        return buffer.getLong();
    }
}