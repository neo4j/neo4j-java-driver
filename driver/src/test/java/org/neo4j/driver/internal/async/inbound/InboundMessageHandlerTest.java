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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.async.ChannelAttributes;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.MessageToByteBufWriter.asByteBuf;
import static org.neo4j.driver.v1.Values.value;

public class InboundMessageHandlerTest
{
    private EmbeddedChannel channel;
    private InboundMessageDispatcher dispatcher;

    @Before
    public void setUp() throws Exception
    {
        channel = new EmbeddedChannel();
        dispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
        ChannelAttributes.setMessageDispatcher( channel, dispatcher );

        InboundMessageHandler handler = new InboundMessageHandler( new PackStreamMessageFormatV1(), DEV_NULL_LOGGING );
        channel.pipeline().addFirst( handler );
    }

    @Test
    public void shouldReadSuccessMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        dispatcher.queue( responseHandler );

        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "key1", value( 1 ) );
        metadata.put( "key2", value( 2 ) );
        channel.writeInbound( asByteBuf( new SuccessMessage( metadata ) ) );

        verify( responseHandler ).onSuccess( metadata );
    }

    @Test
    public void shouldReadFailureMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        dispatcher.queue( responseHandler );

        channel.writeInbound( asByteBuf( new FailureMessage( "Neo.TransientError.General.ReadOnly", "Hi!" ) ) );

        ArgumentCaptor<Neo4jException> captor = ArgumentCaptor.forClass( Neo4jException.class );
        verify( responseHandler ).onFailure( captor.capture() );
        assertEquals( "Neo.TransientError.General.ReadOnly", captor.getValue().code() );
        assertEquals( "Hi!", captor.getValue().getMessage() );
    }

    @Test
    public void shouldReadRecordMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        dispatcher.queue( responseHandler );

        Value[] fields = {value( 1 ), value( 2 ), value( 3 )};
        channel.writeInbound( asByteBuf( new RecordMessage( fields ) ) );

        verify( responseHandler ).onRecord( fields );
    }

    @Test
    public void shouldReadIgnoredMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        dispatcher.queue( responseHandler );

        channel.writeInbound( asByteBuf( new IgnoredMessage() ) );
        assertEquals( 0, dispatcher.queuedHandlersCount() );
    }

    @Test
    public void shouldCloseContextWhenChannelInactive() throws Exception
    {
        assertTrue( channel.isOpen() );

        channel.pipeline().fireChannelInactive();

        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldCloseContextWhenExceptionCaught()
    {
        assertTrue( channel.isOpen() );

        channel.pipeline().fireExceptionCaught( new RuntimeException( "Hi!" ) );

        assertFalse( channel.isOpen() );
    }
}
