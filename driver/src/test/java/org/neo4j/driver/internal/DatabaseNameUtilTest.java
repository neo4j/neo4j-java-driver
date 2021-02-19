/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.DatabaseNameUtil.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.DatabaseNameUtil.systemDatabase;

class DatabaseNameUtilTest
{
    @Test
    void shouldDatabaseNameBeEqual() throws Throwable
    {
        assertEquals( defaultDatabase(), defaultDatabase() );
        assertEquals( defaultDatabase(), database( null ) );
        assertEquals( defaultDatabase(), database( DEFAULT_DATABASE_NAME ) );

        assertEquals( systemDatabase(), systemDatabase() );
        assertEquals( systemDatabase(), database( "system" ) );
        assertEquals( systemDatabase(), database( SYSTEM_DATABASE_NAME ) );

        assertEquals( database( "hello" ), database( "hello" ) );
    }

    @Test
    void shouldReturnDatabaseNameInDescription() throws Throwable
    {
        assertEquals( "<default database>", defaultDatabase().description() );
        assertEquals( "system", systemDatabase().description() );
        assertEquals( "hello", database( "hello" ).description() );
    }
}
