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
package org.neo4j.driver.internal.net;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.driver.internal.BoltServerAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.BoltServerAddress.DEFAULT_PORT;

class BoltServerAddressParsingTest
{
    private static Stream<Arguments> addressesToParse()
    {
        return Stream.of(
                // Hostname
                Arguments.of( "localhost", "localhost", DEFAULT_PORT ),
                Arguments.of( "localhost:9193", "localhost", 9193 ),
                Arguments.of( "neo4j.com", "neo4j.com", DEFAULT_PORT ),
                Arguments.of( "royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT ),
                Arguments.of( "royal-server.com.uk:4546", "royal-server.com.uk", 4546 ),

                // Hostname with scheme
                Arguments.of( "bolt://localhost", "localhost", DEFAULT_PORT ),
                Arguments.of( "neo4j://localhost", "localhost", DEFAULT_PORT ),
                Arguments.of( "bolt://localhost:9193", "localhost", 9193 ),
                Arguments.of( "neo4j://localhost:9193", "localhost", 9193 ),
                Arguments.of( "bolt://neo4j.com", "neo4j.com", DEFAULT_PORT ),
                Arguments.of( "neo4j://neo4j.com", "neo4j.com", DEFAULT_PORT ),
                Arguments.of( "bolt://royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT ),
                Arguments.of( "neo4j://royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT ),
                Arguments.of( "bolt://royal-server.com.uk:4546", "royal-server.com.uk", 4546 ),
                Arguments.of( "neo4j://royal-server.com.uk:4546", "royal-server.com.uk", 4546 ),

                // IPv4
                Arguments.of( "127.0.0.1", "127.0.0.1", DEFAULT_PORT ),
                Arguments.of( "8.8.8.8:8080", "8.8.8.8", 8080 ),
                Arguments.of( "0.0.0.0", "0.0.0.0", DEFAULT_PORT ),
                Arguments.of( "192.0.2.235:4329", "192.0.2.235", 4329 ),
                Arguments.of( "172.31.255.255:255", "172.31.255.255", 255 ),

                // IPv4 with scheme
                Arguments.of( "bolt://198.51.100.0", "198.51.100.0", DEFAULT_PORT ),
                Arguments.of( "bolt://65.21.10.12:5656", "65.21.10.12", 5656 ),
                Arguments.of( "neo4j://12.0.0.5", "12.0.0.5", DEFAULT_PORT ),
                Arguments.of( "neo4j://155.55.20.6:9191", "155.55.20.6", 9191 ),

                // IPv6
                Arguments.of( "::1", "[::1]", DEFAULT_PORT ),
                Arguments.of( "ff02::2:ff00:0", "[ff02::2:ff00:0]", DEFAULT_PORT ),
                Arguments.of( "[1afc:0:a33:85a3::ff2f]", "[1afc:0:a33:85a3::ff2f]", DEFAULT_PORT ),
                Arguments.of( "[::1]:1515", "[::1]", 1515 ),
                Arguments.of( "[ff0a::101]:8989", "[ff0a::101]", 8989 ),

                // IPv6 with scheme
                Arguments.of( "bolt://[::1]", "[::1]", DEFAULT_PORT ),
                Arguments.of( "neo4j://[::1]", "[::1]", DEFAULT_PORT ),
                Arguments.of( "bolt://[ff02::d]", "[ff02::d]", DEFAULT_PORT ),
                Arguments.of( "neo4j://[fe80::b279:2f]", "[fe80::b279:2f]", DEFAULT_PORT ),
                Arguments.of( "bolt://[::1]:8687", "[::1]", 8687 ),
                Arguments.of( "neo4j://[::1]:1212", "[::1]", 1212 ),
                Arguments.of( "bolt://[ff02::d]:9090", "[ff02::d]", 9090 ),
                Arguments.of( "neo4j://[fe80::b279:2f]:7878", "[fe80::b279:2f]", 7878 ),

                // IPv6 with zone id
                Arguments.of( "::1%eth0", "[::1%eth0]", DEFAULT_PORT ),
                Arguments.of( "ff02::2:ff00:0%12", "[ff02::2:ff00:0%12]", DEFAULT_PORT ),
                Arguments.of( "[1afc:0:a33:85a3::ff2f%eth1]", "[1afc:0:a33:85a3::ff2f%eth1]", DEFAULT_PORT ),
                Arguments.of( "[::1%eth0]:3030", "[::1%eth0]", 3030 ),
                Arguments.of( "[ff0a::101%8]:4040", "[ff0a::101%8]", 4040 ),

                // IPv6 with scheme and zone id
                Arguments.of( "bolt://[::1%eth5]", "[::1%eth5]", DEFAULT_PORT ),
                Arguments.of( "neo4j://[::1%12]", "[::1%12]", DEFAULT_PORT ),
                Arguments.of( "bolt://[ff02::d%3]", "[ff02::d%3]", DEFAULT_PORT ),
                Arguments.of( "neo4j://[fe80::b279:2f%eth0]", "[fe80::b279:2f%eth0]", DEFAULT_PORT ),
                Arguments.of( "bolt://[::1%eth3]:8687", "[::1%eth3]", 8687 ),
                Arguments.of( "neo4j://[::1%2]:1212", "[::1%2]", 1212 ),
                Arguments.of( "bolt://[ff02::d%3]:9090", "[ff02::d%3]", 9090 ),
                Arguments.of( "neo4j://[fe80::b279:2f%eth1]:7878", "[fe80::b279:2f%eth1]", 7878 )
        );
    }

    @ParameterizedTest
    @MethodSource( "addressesToParse" )
    void shouldParseAddress( String address, String expectedHost, int expectedPort )
    {
        BoltServerAddress parsed = new BoltServerAddress( address );
        assertEquals( expectedHost, parsed.host() );
        assertEquals( expectedPort, parsed.port() );
    }
}
