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
package org.apache.cassandra.cql3.validation.operations;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;

public class CreateWithTimeOrderedKeyTest extends CQLTester
{
    @Test
    public void testCreateTableWithSmallintColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (t TIMESTAMP, duration_ms INT, a text, b smallint, c smallint, primary key ((t, duration_ms, a), b)) WITH time_ordered_key = true;");
        execute("INSERT INTO %s (t, duration_ms, a, b, c) VALUES (?, ?, ?, ?, ?)", Util.dt(0), 1, "1", (short)1, (short)2);
        execute("INSERT INTO %s (t, duration_ms, a, b, c) VALUES (?, ?, ?, ?, ?)", Util.dt(1), 1, "2", Short.MAX_VALUE, Short.MIN_VALUE);

        assertRows(execute("SELECT * FROM %s"),
                   row(Util.dt(1), 1, "2", Short.MAX_VALUE, Short.MIN_VALUE),
                   row(Util.dt(0), 1, "1", (short) 1, (short) 2));

        assertInvalidMessage("Expected 2 bytes for a smallint (4)",
                             "INSERT INTO %s (t, duration_ms, a, b, c) VALUES (?, ?, ?, ?, ?)", Util.dt(2), 1, "3", 1, 2);
        assertInvalidMessage("Expected 2 bytes for a smallint (0)",
                             "INSERT INTO %s (t, duration_ms, a, b, c) VALUES (?, ?, ?, ?, ?)", Util.dt(2), 1, "3", (short) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }
}
