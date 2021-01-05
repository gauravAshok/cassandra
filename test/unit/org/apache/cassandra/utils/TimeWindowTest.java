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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeWindowTest
{
    @Test
    public void testTestWindowConstructors() {
        TimeWindow tw1 = TimeWindow.fromDuration(99, 31);
        TimeWindow tw2 = TimeWindow.fromLimits(99, 130);

        assertEquals(tw1.start, tw2.start);
        assertEquals(tw1.end, tw2.end);
        assertEquals(tw1.getDuration(), tw2.getDuration());
        assertEquals(tw1, tw2);
    }

    @Test
    public void testTestWindowMethods() {
        TimeWindow tw1 = TimeWindow.fromDuration(99, 32);
        TimeWindow tw2 = TimeWindow.fromDuration(99, 31);
        TimeWindow tw3 = TimeWindow.fromDuration(100, 31);
        TimeWindow tw4 = TimeWindow.fromDuration(110, 1);
        TimeWindow tw5 = TimeWindow.fromDuration(90, 20);
        TimeWindow tw6 = TimeWindow.fromDuration(120, 100);
        TimeWindow tw7 = TimeWindow.fromDuration(90, 9);
        TimeWindow tw8 = TimeWindow.fromDuration(131, 9);

        assertEquals(16, tw1.getWindowLength(2));
        assertEquals(16, tw2.getWindowLength(2));

        assertTrue(tw1.contains(tw2));
        assertTrue(tw1.intersects(tw2));
        assertFalse(tw2.contains(tw1));
        assertTrue(tw2.intersects(tw1));

        assertTrue(tw1.contains(tw3));
        assertTrue(tw1.intersects(tw3));
        assertFalse(tw3.contains(tw1));
        assertTrue(tw3.intersects(tw1));

        assertTrue(tw1.contains(tw4));
        assertTrue(tw1.intersects(tw4));
        assertFalse(tw4.contains(tw1));
        assertTrue(tw4.intersects(tw1));

        assertFalse(tw1.contains(tw5));
        assertTrue(tw1.intersects(tw4));
        assertFalse(tw5.contains(tw1));
        assertTrue(tw4.intersects(tw1));

        assertFalse(tw1.contains(tw6));
        assertTrue(tw1.intersects(tw6));
        assertFalse(tw6.contains(tw1));
        assertTrue(tw6.intersects(tw1));

        assertFalse(tw1.contains(tw7));
        assertFalse(tw1.intersects(tw7));
        assertFalse(tw7.contains(tw1));
        assertFalse(tw7.intersects(tw1));

        assertFalse(tw1.contains(tw8));
        assertFalse(tw1.intersects(tw8));
        assertFalse(tw8.contains(tw1));
        assertFalse(tw8.intersects(tw1));

        assertFalse(tw1.contains(tw8));
        assertFalse(tw1.intersects(tw8));
        assertFalse(tw8.contains(tw1));
        assertFalse(tw8.intersects(tw1));
    }
}
