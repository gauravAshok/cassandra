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
 * Time window representing a time range [ts, ts + duration)
 */
public class TimeWindow
{
    public final long ts;
    public final int duration;

    public TimeWindow(long ts, int duration)
    {
        this.ts = ts;
        this.duration = duration;
    }

    public int getWindowLength(int windowSize)
    {
        return duration / windowSize;
    }

    // end ts is the exclusive bound
    public long getEndTs()
    {
        return ts + duration;
    }

    public boolean intersects(TimeWindow tw) {
        return (tw.ts >= this.ts && tw.ts < getEndTs()) ||
                (tw.getEndTs() > this.ts && tw.getEndTs() <= getEndTs());
    }

    public static TimeWindow merge(List<TimeWindow> windows) {
        long begin = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;

        for(TimeWindow tw : windows) {
            begin = Long.min(begin, tw.ts);
            end = Long.max(end, tw.getEndTs());
        }

        return new TimeWindow(begin, (int)(end - begin));
    }

    public static TimeWindow merge(TimeWindow tw1, TimeWindow tw2) {
        long begin = Long.min(tw1.ts, tw2.ts);
        long end = Long.max(tw1.getEndTs(), tw2.getEndTs());

        return new TimeWindow(begin, (int)(end - begin));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeWindow that = (TimeWindow) o;
        return ts == that.ts &&
                duration == that.duration;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ts, duration);
    }

    @Override
    public String toString()
    {
         return "TimeWindow{" + ts + ':' + duration + '}';
    }
}
