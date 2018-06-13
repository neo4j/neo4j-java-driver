/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.net.SocketAddress;

import org.neo4j.driver.internal.BoltServerAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.driver.internal.BoltServerAddress.DEFAULT_PORT;

class BoltServerAddressTest
{
    @Test
    void defaultPortShouldBe7687()
    {
        assertThat( DEFAULT_PORT, equalTo( 7687 ) );
    }

    @Test
    void portShouldUseDefaultIfNotSupplied()
    {
        assertThat( new BoltServerAddress( "localhost" ).port(), equalTo( BoltServerAddress.DEFAULT_PORT ) );
    }

    @Test
    void shouldAlwaysResolveAddress()
    {
        BoltServerAddress boltAddress = new BoltServerAddress( "localhost" );

        SocketAddress socketAddress1 = boltAddress.toSocketAddress();
        SocketAddress socketAddress2 = boltAddress.toSocketAddress();

        assertNotSame( socketAddress1, socketAddress2 );
    }

    @Test
    void shouldHaveCorrectToString()
    {
        assertEquals( "localhost:4242", new BoltServerAddress( "localhost", 4242 ).toString() );
        assertEquals( "127.0.0.1:8888", new BoltServerAddress( "127.0.0.1", 8888 ).toString() );
    }
}
