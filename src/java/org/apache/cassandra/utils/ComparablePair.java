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

public class ComparablePair<T1 extends Comparable<T1>, T2 extends Comparable<T2>> extends Pair<T1, T2> implements Comparable<ComparablePair<T1, T2>> {
    protected ComparablePair(T1 left, T2 right) {
        super(left, right);
    }

    @Override
    public int compareTo(ComparablePair<T1, T2> o) {
        if (o == null) {
            return 1;
        }
        int leftCompare = this.left.compareTo(o.left);
        if (leftCompare != 0) {
            return leftCompare;
        }
        return this.right.compareTo(o.right);
    }

    public static <X extends Comparable<X>, Y extends Comparable<Y>> ComparablePair<X, Y> create(X left, Y right) {
        return new ComparablePair<>(left, right);
    }
}
