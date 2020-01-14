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

package org.apache.cassandra.utils;

import java.util.List;
import java.util.Objects;

/**
 * Time window representing a range [start, end). Use it with ms only to avoid any bugs.
 */
public class TimeWindow
{
    public static final TimeWindow ALL = TimeWindow.fromLimits(0, Long.MAX_VALUE);

    public final long start;
    public final long end;

    private TimeWindow(long start, long end)
    {
        this.start = start;
        this.end = end;
    }

    public static TimeWindow fromDuration(long start, long duration)
    {
        return new TimeWindow(start, start + duration);
    }

    public static TimeWindow fromLimits(long start, long end)
    {
        return new TimeWindow(start, end);
    }

    public long getWindowLength(long windowSize)
    {
        return getDuration() / windowSize;
    }

    // end ts is the exclusive bound
    public long getEnd()
    {
        return end;
    }

    public long getStart()
    {
        return start;
    }

    public long getDuration()
    {
        return end - start;
    }

    public boolean intersects(TimeWindow tw)
    {
        if (ALL.equals(tw) || ALL.equals(this))
        {
            return true;
        }
        return tw.start < this.end && tw.end > this.start;
    }

    public boolean contains(TimeWindow tw)
    {
        if (ALL.equals(this))
        {
            return true;
        }

        return start >= this.start && end <= this.end;
    }

    public static TimeWindow merge(List<TimeWindow> windows)
    {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;

        for (TimeWindow tw : windows)
        {
            start = Long.min(start, tw.start);
            end = Long.max(end, tw.end);
        }

        return TimeWindow.fromLimits(start, end);
    }

    public static TimeWindow merge(TimeWindow tw1, TimeWindow tw2)
    {
        long start = Long.min(tw1.start, tw2.start);
        long end = Long.max(tw1.end, tw2.end);

        return TimeWindow.fromLimits(start, end);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeWindow that = (TimeWindow) o;
        return start == that.start &&
               end == that.end;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end);
    }

    @Override
    public String toString()
    {
        return "TimeWindow{" + start + '-' + end + '}';
    }
}
