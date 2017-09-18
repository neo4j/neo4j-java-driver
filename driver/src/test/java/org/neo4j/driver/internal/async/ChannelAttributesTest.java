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
package org.neo4j.driver.internal.async;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.async.ChannelAttributes.address;
import static org.neo4j.driver.internal.async.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.serverVersion;
import static org.neo4j.driver.internal.async.ChannelAttributes.setAddress;
import static org.neo4j.driver.internal.async.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.setServerVersion;

public class ChannelAttributesTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldSetAndGetAddress()
    {
        BoltServerAddress address = new BoltServerAddress( "local:42" );
        setAddress( channel, address );
        assertEquals( address, address( channel ) );
    }

    @Test
    public void shouldFailToSetAddressTwice()
    {
        setAddress( channel, BoltServerAddress.LOCAL_DEFAULT );

        try
        {
            setAddress( channel, BoltServerAddress.LOCAL_DEFAULT );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldSetAndGetCreationTimestamp()
    {
        setCreationTimestamp( channel, 42L );
        assertEquals( 42L, creationTimestamp( channel ) );
    }

    @Test
    public void shouldFailToSetCreationTimestampTwice()
    {
        setCreationTimestamp( channel, 42L );

        try
        {
            setCreationTimestamp( channel, 42L );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldSetAndGetLastUsedTimestamp()
    {
        assertNull( lastUsedTimestamp( channel ) );
        setLastUsedTimestamp( channel, 42L );
        assertEquals( 42L, lastUsedTimestamp( channel ).longValue() );
    }

    @Test
    public void shouldAllowSettingLastUsedTimestampMultipleTimes()
    {
        setLastUsedTimestamp( channel, 42L );
        setLastUsedTimestamp( channel, 4242L );
        setLastUsedTimestamp( channel, 424242L );

        assertEquals( 424242L, lastUsedTimestamp( channel ).longValue() );
    }

    @Test
    public void shouldSetAndGetMessageDispatcher()
    {
        InboundMessageDispatcher dispatcher = mock( InboundMessageDispatcher.class );
        setMessageDispatcher( channel, dispatcher );
        assertEquals( dispatcher, messageDispatcher( channel ) );
    }

    @Test
    public void shouldFailToSetMessageDispatcherTwice()
    {
        setMessageDispatcher( channel, mock( InboundMessageDispatcher.class ) );

        try
        {
            setMessageDispatcher( channel, mock( InboundMessageDispatcher.class ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldSetAndGetServerVersion()
    {
        setServerVersion( channel, "3.2.1" );
        assertEquals( "3.2.1", serverVersion( channel ) );
    }

    @Test
    public void shouldFailToSetServerVersionTwice()
    {
        setServerVersion( channel, "3.2.2" );

        try
        {
            setServerVersion( channel, "3.2.3" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }
}
