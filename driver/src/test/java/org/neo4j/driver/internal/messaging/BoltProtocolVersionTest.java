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
package org.neo4j.driver.internal.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoltProtocolVersionTest
{

    @ParameterizedTest( name = "V{0}.{1}" )
    @CsvSource( {"3, 0", "4, 0", "4, 1", "4, 2", "100, 100", "255, 255", "0, 0"} )
    void shouldParseVersion( int major, int minor )
    {
        BoltProtocolVersion protocolVersion = new BoltProtocolVersion( major, minor );

        BoltProtocolVersion testVersion = BoltProtocolVersion.fromRawBytes( protocolVersion.toInt() );

        assertEquals( major, testVersion.getMajorVersion() );
        assertEquals( minor, testVersion.getMinorVersion() );
    }

    @ParameterizedTest( name = "V{0}.{1} comparedTo V{2}.{3}" )
    @CsvSource( {"1, 3, 25, 21, -1", "4, 0, 4, 0, 0", "4, 1, 4, 0, 1", "0, 1, 0, 2, -1"} )
    void shouldCompareTo(int majorA, int minorA, int majorB, int minorB, int expectedResult)
    {
        BoltProtocolVersion versionA = new BoltProtocolVersion( majorA, minorA );
        BoltProtocolVersion versionB = new BoltProtocolVersion( majorB, minorB );

        assertEquals( expectedResult, versionA.compareTo( versionB ) );

    }

    @Test
    void shouldOutputCorrectLongFormatForMajorVersionOnly()
    {
        BoltProtocolVersion version = new BoltProtocolVersion( 4, 0 );
        assertEquals( 4L, version.toInt() );
    }

    @Test
    void shouldOutputCorrectLongFormatForMajorAndMinorVersion()
    {
        BoltProtocolVersion version = new BoltProtocolVersion( 4, 1 );
        assertEquals( 260L, version.toInt() );
    }

    @Test
    void shouldOutputFormattedString()
    {
        BoltProtocolVersion version = new BoltProtocolVersion( 4, 1 );

        assertEquals( "4.1", version.toString() );
    }

}