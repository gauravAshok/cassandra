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

package org.apache.cassandra.repair;

// TODO: merge this class with TimeWindow
public class TimeRange
{
    public static final TimeRange DEFAULT = new TimeRange(0, Long.MAX_VALUE);

    public final long start;
    public final long end;

    public TimeRange(long start, long end)
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString()
    {
        return "[" + start + ", " + end + ')';
    }

    public boolean intersects(TimeRange timeRange)
    {
        return intersects(timeRange.start, timeRange.end);
    }

    public boolean intersects(long start, long end)
    {
        return start < this.end && end > this.start;
    }

    public boolean contains(long start, long end)
    {
        return start >= this.start && end <= this.end;
    }
}
