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
package org.neo4j.driver.internal.async.connection;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.TestUtil.await;

class ChannelConnectedListenerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailPromiseWhenChannelConnectionFails()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        ChannelConnectedListener listener = newListener( handshakeCompletedPromise );

        ChannelPromise channelConnectedPromise = channel.newPromise();
        IOException cause = new IOException( "Unable to connect!" );
        channelConnectedPromise.setFailure( cause );

        listener.operationComplete( channelConnectedPromise );

        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( cause, error.getCause() );
    }

    @Test
    void shouldWriteHandshakeWhenChannelConnected()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        ChannelConnectedListener listener = newListener( handshakeCompletedPromise );

        ChannelPromise channelConnectedPromise = channel.newPromise();
        channelConnectedPromise.setSuccess();

        listener.operationComplete( channelConnectedPromise );

        assertNotNull( channel.pipeline().get( HandshakeHandler.class ) );
        assertTrue( channel.finish() );
        assertEquals( handshakeBuf(), channel.readOutbound() );
    }

    private static ChannelConnectedListener newListener( ChannelPromise handshakeCompletedPromise )
    {
        return new ChannelConnectedListener( LOCAL_DEFAULT, new ChannelPipelineBuilderImpl(),
                handshakeCompletedPromise, DEV_NULL_LOGGING );
    }
}
