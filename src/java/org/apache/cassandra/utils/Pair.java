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

import com.google.common.base.Objects;

public class Pair<T1, T2>
{
    public final T1 left;
    public final T2 right;

    protected Pair(T1 left, T2 right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public final int hashCode()
    {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31*hashCode + (right == null ? 0 : right.hashCode());
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof Pair))
            return false;
        Pair that = (Pair)o;
        // handles nulls properly
        return Objects.equal(left, that.left) && Objects.equal(right, that.right);
    }

    @Override
    public String toString()
    {
        return "(" + left + "," + right + ")";
    }

    public static <X, Y> Pair<X, Y> create(X x, Y y)
    {
        return new Pair<X, Y>(x, y);
    }

    public static class Comparator<T1, T2> implements java.util.Comparator<Pair<T1, T2>> {

        private final java.util.Comparator<T1> leftComparator;
        private final java.util.Comparator<T2> rightComparator;

        Comparator(java.util.Comparator<T1> leftComparator, java.util.Comparator<T2> rightComparator) {
            this.leftComparator = leftComparator;
            this.rightComparator = rightComparator;
        }

        @Override
        public int compare(Pair<T1, T2> o1, Pair<T1, T2> o2)
        {
            if(o1 == o2)
                return 0;

            if (o2 == null) {
                return 1;
            } else if(o1 == null) {
                return -1;
            }

            int t1Compare = leftComparator.compare(o1.left, o2.left);

            if(t1Compare == 0) {
                return rightComparator.compare(o1.right, o2.right);
            }

            return t1Compare;
        }
    }
}
