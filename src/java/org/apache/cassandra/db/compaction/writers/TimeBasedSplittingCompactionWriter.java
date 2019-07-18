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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TimeBasedSplittingCompactionWriter extends CompactionAwareWriter
{

    private final int windowSizeInMin;
    private final long windowStartInMin;
    private final int windowCount;
    private final Set<SSTableReader> allSSTables;
    private SSTableWriter[] ssTableWriters;
    private Directories.DataDirectory sstableDirectory;

    public TimeBasedSplittingCompactionWriter(
        ColumnFamilyStore cfs,
        Directories directories,
        LifecycleTransaction txn,
        Set<SSTableReader> nonExpiredSSTables,
        boolean keepOriginals,
        int windowSizeInMin,
        long windowStartInMin,
        int windowCount) {

        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.windowSizeInMin = windowSizeInMin;
        this.windowStartInMin = windowStartInMin;
        this.windowCount = windowCount;
        this.allSSTables = txn.originals();
        this.ssTableWriters = new SSTableWriter[windowCount];
    }

    @Override
    protected boolean realAppend(UnfilteredRowIterator partition) {

        int windowIndex = getWindowIndex(partition.partitionKey());

        if (ssTableWriters[windowIndex] == null) {
            ssTableWriters[windowIndex] = getSSTableWriter();
        }

        if (sstableWriter.currentWriter() != ssTableWriters[windowIndex]) {
            sstableWriter.switchWriter(ssTableWriters[windowIndex]);
        }

        RowIndexEntry rie = sstableWriter.append(partition);
        return rie != null;
    }

    private int getWindowIndex(DecoratedKey pk) {

        ByteBuffer buffer = pk.getKey().duplicate();
        buffer.getShort();
        long ts = buffer.getLong();
        int delta = (int) (TimeUnit.MINUTES.convert(ts, TimeUnit.MILLISECONDS) - windowStartInMin);
        return delta / windowSizeInMin;
    }

    @Override
    protected void switchCompactionLocation(DataDirectory directory) {

        sstableDirectory = directory;
        ssTableWriters = new SSTableWriter[windowCount];
    }

    @SuppressWarnings("resource")
    private SSTableWriter getSSTableWriter() {

        return SSTableWriter.create(
            Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(sstableDirectory))),
            estimatedTotalKeys / windowCount,
            minRepairedAt,
            cfs.metadata,
            new MetadataCollector(allSSTables, cfs.metadata.comparator, 0),
            SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
            cfs.indexManager.listIndexes(),
            txn);
    }
}
