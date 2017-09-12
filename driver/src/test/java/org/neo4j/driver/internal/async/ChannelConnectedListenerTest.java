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

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.async.ProtocolUtil.handshake;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class ChannelConnectedListenerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldFailPromiseWhenChannelConnectionFails()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        ChannelConnectedListener listener = newListener( handshakeCompletedPromise );

        ChannelPromise channelConnectedPromise = channel.newPromise();
        IOException cause = new IOException( "Unable to connect!" );
        channelConnectedPromise.setFailure( cause );

        listener.operationComplete( channelConnectedPromise );

        try
        {
            await( handshakeCompletedPromise );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertEquals( cause, e.getCause() );
        }
    }

    @Test
    public void shouldWriteHandshakeWhenChannelConnected()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        ChannelConnectedListener listener = newListener( handshakeCompletedPromise );

        ChannelPromise channelConnectedPromise = channel.newPromise();
        channelConnectedPromise.setSuccess();

        listener.operationComplete( channelConnectedPromise );

        assertNotNull( channel.pipeline().get( HandshakeResponseHandler.class ) );
        assertTrue( channel.finish() );
        assertEquals( handshake(), channel.readOutbound() );
    }

    private static ChannelConnectedListener newListener( ChannelPromise handshakeCompletedPromise )
    {
        return new ChannelConnectedListener( LOCAL_DEFAULT, handshakeCompletedPromise, DEV_NULL_LOGGING );
    }
}
