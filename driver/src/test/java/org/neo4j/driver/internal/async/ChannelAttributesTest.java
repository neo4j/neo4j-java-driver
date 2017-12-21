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
import org.junit.Test;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.util.ServerVersion;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.async.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.serverAddress;
import static org.neo4j.driver.internal.async.ChannelAttributes.serverVersion;
import static org.neo4j.driver.internal.async.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.setServerAddress;
import static org.neo4j.driver.internal.async.ChannelAttributes.setServerVersion;
import static org.neo4j.driver.internal.async.ChannelAttributes.setTerminationReason;
import static org.neo4j.driver.internal.async.ChannelAttributes.terminationReason;
import static org.neo4j.driver.internal.util.ServerVersion.version;

public class ChannelAttributesTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @Test
    public void shouldSetAndGetAddress()
    {
        BoltServerAddress address = new BoltServerAddress( "local:42" );
        setServerAddress( channel, address );
        assertEquals( address, serverAddress( channel ) );
    }

    @Test
    public void shouldFailToSetAddressTwice()
    {
        setServerAddress( channel, BoltServerAddress.LOCAL_DEFAULT );

        try
        {
            setServerAddress( channel, BoltServerAddress.LOCAL_DEFAULT );
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
        ServerVersion version = version( "3.2.1" );
        setServerVersion( channel, version );
        assertEquals( version, serverVersion( channel ) );
    }

    @Test
    public void shouldFailToSetServerVersionTwice()
    {
        setServerVersion( channel, version( "3.2.2" ) );

        try
        {
            setServerVersion( channel, version( "3.2.3" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldSetAndGetTerminationReason()
    {
        String reason = "This channel has been terminated";
        setTerminationReason( channel, reason );
        assertEquals( reason, terminationReason( channel ) );
    }

    @Test
    public void shouldFailToSetTerminationReasonTwice()
    {
        setTerminationReason( channel, "Reason 1" );

        try
        {
            setTerminationReason( channel, "Reason 2" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }
}
