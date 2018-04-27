/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.neo4j.driver.internal.BoltServerAddress;

import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.internal.BoltServerAddress.DEFAULT_PORT;

@RunWith( Parameterized.class )
public class BoltServerAddressParsingTest
{
    @Parameter
    public String address;
    @Parameter( 1 )
    public String expectedHost;
    @Parameter( 2 )
    public int expectedPort;

    @Parameters( name = "{0}" )
    public static Object[][] addressesToParse()
    {
        return new Object[][]{
                // Hostname
                {"localhost", "localhost", DEFAULT_PORT},
                {"localhost:9193", "localhost", 9193},
                {"neo4j.com", "neo4j.com", DEFAULT_PORT},
                {"royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT},
                {"royal-server.com.uk:4546", "royal-server.com.uk", 4546},

                // Hostname with scheme
                {"bolt://localhost", "localhost", DEFAULT_PORT},
                {"bolt+routing://localhost", "localhost", DEFAULT_PORT},
                {"bolt://localhost:9193", "localhost", 9193},
                {"bolt+routing://localhost:9193", "localhost", 9193},
                {"bolt://neo4j.com", "neo4j.com", DEFAULT_PORT},
                {"bolt+routing://neo4j.com", "neo4j.com", DEFAULT_PORT},
                {"bolt://royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT},
                {"bolt+routing://royal-server.com.uk", "royal-server.com.uk", DEFAULT_PORT},
                {"bolt://royal-server.com.uk:4546", "royal-server.com.uk", 4546},
                {"bolt+routing://royal-server.com.uk:4546", "royal-server.com.uk", 4546},

                // IPv4
                {"127.0.0.1", "127.0.0.1", DEFAULT_PORT},
                {"8.8.8.8:8080", "8.8.8.8", 8080},
                {"0.0.0.0", "0.0.0.0", DEFAULT_PORT},
                {"192.0.2.235:4329", "192.0.2.235", 4329},
                {"172.31.255.255:255", "172.31.255.255", 255},

                // IPv4 with scheme
                {"bolt://198.51.100.0", "198.51.100.0", DEFAULT_PORT},
                {"bolt://65.21.10.12:5656", "65.21.10.12", 5656},
                {"bolt+routing://12.0.0.5", "12.0.0.5", DEFAULT_PORT},
                {"bolt+routing://155.55.20.6:9191", "155.55.20.6", 9191},

                // IPv6
                {"::1", "[::1]", DEFAULT_PORT},
                {"ff02::2:ff00:0", "[ff02::2:ff00:0]", DEFAULT_PORT},
                {"[1afc:0:a33:85a3::ff2f]", "[1afc:0:a33:85a3::ff2f]", DEFAULT_PORT},
                {"[::1]:1515", "[::1]", 1515},
                {"[ff0a::101]:8989", "[ff0a::101]", 8989},

                // IPv6 with scheme
                {"bolt://[::1]", "[::1]", DEFAULT_PORT},
                {"bolt+routing://[::1]", "[::1]", DEFAULT_PORT},
                {"bolt://[ff02::d]", "[ff02::d]", DEFAULT_PORT},
                {"bolt+routing://[fe80::b279:2f]", "[fe80::b279:2f]", DEFAULT_PORT},
                {"bolt://[::1]:8687", "[::1]", 8687},
                {"bolt+routing://[::1]:1212", "[::1]", 1212},
                {"bolt://[ff02::d]:9090", "[ff02::d]", 9090},
                {"bolt+routing://[fe80::b279:2f]:7878", "[fe80::b279:2f]", 7878},

                // IPv6 with zone id
                {"::1%eth0", "[::1%eth0]", DEFAULT_PORT},
                {"ff02::2:ff00:0%12", "[ff02::2:ff00:0%12]", DEFAULT_PORT},
                {"[1afc:0:a33:85a3::ff2f%eth1]", "[1afc:0:a33:85a3::ff2f%eth1]", DEFAULT_PORT},
                {"[::1%eth0]:3030", "[::1%eth0]", 3030},
                {"[ff0a::101%8]:4040", "[ff0a::101%8]", 4040},

                // IPv6 with scheme and zone id
                {"bolt://[::1%eth5]", "[::1%eth5]", DEFAULT_PORT},
                {"bolt+routing://[::1%12]", "[::1%12]", DEFAULT_PORT},
                {"bolt://[ff02::d%3]", "[ff02::d%3]", DEFAULT_PORT},
                {"bolt+routing://[fe80::b279:2f%eth0]", "[fe80::b279:2f%eth0]", DEFAULT_PORT},
                {"bolt://[::1%eth3]:8687", "[::1%eth3]", 8687},
                {"bolt+routing://[::1%2]:1212", "[::1%2]", 1212},
                {"bolt://[ff02::d%3]:9090", "[ff02::d%3]", 9090},
                {"bolt+routing://[fe80::b279:2f%eth1]:7878", "[fe80::b279:2f%eth1]", 7878},
        };
    }

    @Test
    public void shouldParseAddress()
    {
        BoltServerAddress parsed = new BoltServerAddress( address );
        assertEquals( expectedHost, parsed.host() );
        assertEquals( expectedPort, parsed.port() );
    }
}
