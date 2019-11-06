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
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TimeBasedSplittingCompactionWriter extends CompactionAwareWriter
{
    private final int windowSizeInSec;
    private final int splitWindowCount;
    private final long windowStartInSec;
    private final int windowCount;
    private final int level;
    private final Set<SSTableReader> allSSTables;
    private SSTableWriter[] ssTableWriters;
    private Directories.DataDirectory sstableDirectory;

    public TimeBasedSplittingCompactionWriter(
            ColumnFamilyStore cfs,
            Directories directories,
            LifecycleTransaction txn,
            Set<SSTableReader> nonExpiredSSTables,
            boolean keepOriginals,
            int windowSizeInSec,
            long windowStartInSec,
            int windowCount,
            int level,
            int splitFactor)
    {

        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.windowSizeInSec = windowSizeInSec;
        this.windowStartInSec = windowStartInSec;
        this.windowCount = windowCount;
        this.allSSTables = txn.originals();
        this.level = level;
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
        long delta = TimeUnit.SECONDS.convert(ts, TimeUnit.MILLISECONDS) - windowStartInSec;
        int window = (int)(delta / windowSizeInSec);
        return window / splitWindowCount;
    }

    @Override
    protected void switchCompactionLocation(DataDirectory directory)
    {
        // TODO: may have to consider the situation where directory is repeated.
        sstableDirectory = directory;
        ssTableWriters = new SSTableWriter[windowCount];
    }

    @SuppressWarnings("resource")
    private SSTableWriter getSSTableWriter()
    {
        return SSTableWriter.create(
                Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(sstableDirectory))),
                estimatedTotalKeys / windowCount,
                minRepairedAt,
                cfs.metadata,
                new MetadataCollector(allSSTables, cfs.metadata.comparator, cfs.timeOrderedKey(), level),
                SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                cfs.indexManager.listIndexes(),
                txn);
    }
}
