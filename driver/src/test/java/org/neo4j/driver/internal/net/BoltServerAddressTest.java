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

import java.net.SocketAddress;

import org.neo4j.driver.internal.BoltServerAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.BoltServerAddress.DEFAULT_PORT;

public class BoltServerAddressTest
{
    @Test
    public void defaultPortShouldBe7687()
    {
        assertThat( DEFAULT_PORT, equalTo( 7687 ) );
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
    public void shouldHaveCorrectToString()
    {
        assertEquals( "localhost:4242", new BoltServerAddress( "localhost", 4242 ).toString() );
        assertEquals( "127.0.0.1:8888", new BoltServerAddress( "127.0.0.1", 8888 ).toString() );
    }
}
