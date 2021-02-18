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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;

import static java.lang.Integer.MAX_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerVersionTest
{
    @Test
    void version()
    {
        assertThat( ServerVersion.version( "Neo4j/dev" ), is( ServerVersion.vInDev ) );
        assertThat( ServerVersion.version( "Neo4j/4.0.0" ), is( ServerVersion.v4_0_0 ) );
    }

    @Test
    void shouldHaveCorrectToString()
    {
        assertEquals( "Neo4j/dev", ServerVersion.vInDev.toString() );
        assertEquals( "Neo4j/4.0.0", ServerVersion.v4_0_0.toString() );
        assertEquals( "Neo4j/3.5.0", ServerVersion.v3_5_0.toString() );
        assertEquals( "Neo4j/3.5.7", ServerVersion.version( "Neo4j/3.5.7" ).toString() );
    }

    @Test
    void shouldFailToParseIllegalVersions()
    {
        assertThrows( IllegalArgumentException.class, () -> ServerVersion.version( "" ) );
        assertThrows( IllegalArgumentException.class, () -> ServerVersion.version( "/1.2.3" ) );
        assertThrows( IllegalArgumentException.class, () -> ServerVersion.version( "Neo4j1.2.3" ) );
        assertThrows( IllegalArgumentException.class, () -> ServerVersion.version( "Neo4j" ) );
    }

    @Test
    void shouldFailToCompareDifferentProducts()
    {
        ServerVersion version1 = ServerVersion.version( "MyNeo4j/1.2.3" );
        ServerVersion version2 = ServerVersion.version( "OtherNeo4j/1.2.4" );

        assertThrows( IllegalArgumentException.class, () -> version1.greaterThanOrEqual( version2 ) );
    }

    @Test
    void shouldReturnCorrectServerVersionFromBoltProtocolVersion()
    {
        assertEquals( ServerVersion.v4_0_0, ServerVersion.fromBoltProtocolVersion( BoltProtocolV4.VERSION ) );
        assertEquals( ServerVersion.v4_1_0, ServerVersion.fromBoltProtocolVersion( BoltProtocolV41.VERSION ) );
        assertEquals( ServerVersion.v4_2_0, ServerVersion.fromBoltProtocolVersion( BoltProtocolV42.VERSION ) );
        assertEquals( ServerVersion.vInDev, ServerVersion.fromBoltProtocolVersion( new BoltProtocolVersion( MAX_VALUE, MAX_VALUE ) ) );
    }
}
