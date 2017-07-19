/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.net.SocketAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class BoltServerAddressTest
{
    @Test
    public void defaultPortShouldBe7687()
    {
        assertThat( BoltServerAddress.DEFAULT_PORT, equalTo( 7687 ) );
    }

    @Test
    public void portShouldUseDefaultIfNotSupplied()
    {
        assertThat( new BoltServerAddress( "localhost" ).port(), equalTo( BoltServerAddress.DEFAULT_PORT ) );
    }

    @Test
    public void shouldAlwaysResolveAddress()
    {
        BoltServerAddress boltAddress = new BoltServerAddress( "localhost" );

        SocketAddress socketAddress1 = boltAddress.toSocketAddress();
        SocketAddress socketAddress2 = boltAddress.toSocketAddress();

        assertNotSame( socketAddress1, socketAddress2 );
    }

    @Test
    public void shouldParseIPv4Addresses()
    {
        BoltServerAddress address1 = new BoltServerAddress( "127.0.0.1:1234" );
        assertEquals( "127.0.0.1", address1.host() );
        assertEquals( 1234, address1.port() );

        BoltServerAddress address2 = new BoltServerAddress( "8.8.8.8:8080" );
        assertEquals( "8.8.8.8", address2.host() );
        assertEquals( 8080, address2.port() );
    }

    @Test
    public void shouldParseIPv6Addresses()
    {
        BoltServerAddress address1 = new BoltServerAddress( "[::1]:7688" );
        assertEquals( "[::1]", address1.host() );
        assertEquals( 7688, address1.port() );

        BoltServerAddress address2 = new BoltServerAddress( "[1afc:0:a33:85a3::ff2f]:9001" );
        assertEquals( "[1afc:0:a33:85a3::ff2f]", address2.host() );
        assertEquals( 9001, address2.port() );
    }

    @Test
    public void shouldParseBoltAddresses()
    {
        BoltServerAddress address = new BoltServerAddress( "bolt://host:6565" );

        assertEquals( "host", address.host() );
        assertEquals( 6565, address.port() );
    }
}
