/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.bolt.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.systemDatabase;

import org.junit.jupiter.api.Test;

class DatabaseNameUtilTest {
    @Test
    @SuppressWarnings("EqualsWithItself")
    void shouldDatabaseNameBeEqual() {
        assertEquals(defaultDatabase(), defaultDatabase());
        assertEquals(defaultDatabase(), database(null));
        assertEquals(defaultDatabase(), database(DEFAULT_DATABASE_NAME));

        assertEquals(systemDatabase(), systemDatabase());
        assertEquals(systemDatabase(), database("system"));
        assertEquals(systemDatabase(), database(SYSTEM_DATABASE_NAME));

        assertEquals(database("hello"), database("hello"));
    }

    @Test
    void shouldReturnDatabaseNameInDescription() {
        assertEquals("<default database>", defaultDatabase().description());
        assertEquals("system", systemDatabase().description());
        assertEquals("hello", database("hello").description());
    }
}
