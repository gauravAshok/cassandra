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

package org.apache.cassandra.db.compaction.writers;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.TimeOrderedKeyCompactionStrategy;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.TimeWindow;

import java.util.Set;
import java.util.stream.Collectors;

public class TimeBasedSplittingCompactionWriter extends CompactionAwareWriter
{
    private final long compactionWindowSizeInMs;
    private final long windowStartInMs;
    // length of split window in terms of compaction windows. splitWindowCount = 2 means window_size = 2 * compactionWindowSizeInMs
    private final int splitWindowCount;
    private final int level;
    private final int splitFactor;
    private final Set<SSTableReader> allSSTables;
    private SSTableWriter[] ssTableWriters;
    private Directories.DataDirectory sstableDirectory;

    public TimeBasedSplittingCompactionWriter(ColumnFamilyStore cfs,
                                              Directories directories,
                                              LifecycleTransaction txn,
                                              Set<SSTableReader> nonExpiredSSTables,
                                              boolean keepOriginals,
                                              long compactionWindowSizeInMs,
                                              int level,
                                              int splitFactor)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.allSSTables = txn.originals();
        this.level = level;
        this.splitFactor = splitFactor;

        TimeWindow tw = TimeWindow.merge(
                txn.originals().stream()
                   .map(s -> TimeOrderedKeyCompactionStrategy.getTimeWindowMs(s, compactionWindowSizeInMs))
                   .collect(Collectors.toList()));
        this.compactionWindowSizeInMs = compactionWindowSizeInMs;
        this.windowStartInMs = tw.ts;
        int windowCount = (int) tw.getWindowLength(compactionWindowSizeInMs);
        this.splitWindowCount = windowCount % splitFactor == 0 ? windowCount / splitFactor : 1 + windowCount / splitFactor;
        this.ssTableWriters = new SSTableWriter[splitFactor];
    }

    @Override
    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        int windowIndex = getWindowIndex(partition.partitionKey());

        if (ssTableWriters[windowIndex] == null)
        {
            ssTableWriters[windowIndex] = getSSTableWriter();
        }

        if (sstableWriter.currentWriter() != ssTableWriters[windowIndex])
        {
            sstableWriter.switchWriter(ssTableWriters[windowIndex]);
        }

        RowIndexEntry rie = sstableWriter.append(partition);
        return rie != null;
    }

    @VisibleForTesting
    int getWindowIndex(DecoratedKey pk)
    {
        long ts = pk.interpretTimeBucket().ts;
        long delta = ts - windowStartInMs;
        int window = (int) (delta / compactionWindowSizeInMs);
        return window / splitWindowCount;
    }

    @Override
    protected void switchCompactionLocation(DataDirectory directory)
    {
        // TODO: may have to consider the situation where directory is repeated.
        sstableDirectory = directory;
        ssTableWriters = new SSTableWriter[splitFactor];
    }

    @SuppressWarnings("resource")
    private SSTableWriter getSSTableWriter()
    {
        return SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                                    estimatedTotalKeys / splitFactor,
                                    minRepairedAt,
                                    pendingRepair,
                                    isTransient,
                                    cfs.metadata,
                                    new MetadataCollector(allSSTables, cfs.metadata().comparator, cfs.metadata().params.timeOrderedKey, level),
                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                    cfs.indexManager.listIndexes(),
                                    txn);
    }
}
