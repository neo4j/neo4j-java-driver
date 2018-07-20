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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.request.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.v1.Values.value;

class InboundMessageDispatcherTest
{
    private static final String FAILURE_CODE = "Neo.ClientError.Security.Unauthorized";
    private static final String FAILURE_MESSAGE = "Error Message";

    @Test
    void shouldFailWhenCreatedWithNullChannel()
    {
        assertThrows( NullPointerException.class, () -> new InboundMessageDispatcher( null, DEV_NULL_LOGGING ) );
    }

    @Test
    void shouldFailWhenCreatedWithNullLogging()
    {
        assertThrows( NullPointerException.class, () -> new InboundMessageDispatcher( mock( Channel.class ), null ) );
    }

    @Test
    void shouldDequeHandlerOnSuccess()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "key1", value( 1 ) );
        metadata.put( "key2", value( "2" ) );
        dispatcher.handleSuccessMessage( metadata );

        assertEquals( 0, dispatcher.queuedHandlersCount() );
        verify( handler ).onSuccess( metadata );
    }

    @Test
    void shouldDequeHandlerOnFailure()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        // ACK_FAILURE handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler );
        assertEquals( FAILURE_CODE, ((Neo4jException) dispatcher.currentError()).code() );
        assertEquals( FAILURE_MESSAGE, dispatcher.currentError().getMessage() );
    }

    @Test
    void shouldSendAckFailureOnFailure()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    void shouldNotSendAckFailureOnFailureWhenMuted()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );
        dispatcher.muteAckFailure();

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel, never() ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    void shouldUnMuteAckFailureWhenNotMuted()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.unMuteAckFailure();

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    void shouldSendAckFailureAfterUnMute()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );
        dispatcher.muteAckFailure();

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel, never() ).writeAndFlush( eq( ACK_FAILURE ), any() );

        dispatcher.unMuteAckFailure();

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel, times( 1 ) ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    void shouldClearFailureOnAckFailureSuccess()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        dispatcher.handleSuccessMessage( emptyMap() );

        assertNull( dispatcher.currentError() );
    }

    @Test
    void shouldPeekHandlerOnRecord()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        Value[] fields1 = {new IntegerValue( 1 )};
        Value[] fields2 = {new IntegerValue( 2 )};
        Value[] fields3 = {new IntegerValue( 3 )};

        dispatcher.handleRecordMessage( fields1 );
        dispatcher.handleRecordMessage( fields2 );
        dispatcher.handleRecordMessage( fields3 );

        verify( handler ).onRecord( fields1 );
        verify( handler ).onRecord( fields2 );
        verify( handler ).onRecord( fields3 );
        assertEquals( 1, dispatcher.queuedHandlersCount() );
    }

    @Test
    void shouldFailAllHandlersOnFatalError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );
        ResponseHandler handler3 = mock( ResponseHandler.class );

        dispatcher.queue( handler1 );
        dispatcher.queue( handler2 );
        dispatcher.queue( handler3 );

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleFatalError( fatalError );

        InOrder inOrder = inOrder( handler1, handler2, handler3 );
        inOrder.verify( handler1 ).onFailure( fatalError );
        inOrder.verify( handler2 ).onFailure( fatalError );
        inOrder.verify( handler3 ).onFailure( fatalError );
    }

    @Test
    void shouldFailNewHandlerAfterFatalError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleFatalError( fatalError );

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );

        verify( handler ).onFailure( fatalError );
    }

    @Test
    void shouldDequeHandlerOnIgnored()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );

        dispatcher.queue( handler );
        dispatcher.handleIgnoredMessage();

        assertEquals( 0, dispatcher.queuedHandlersCount() );
    }

    @Test
    void shouldFailHandlerOnIgnoredMessageWithExistingError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.queue( handler1 );
        dispatcher.queue( handler2 );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verifyFailure( handler1 );
        verifyZeroInteractions( handler2 );

        dispatcher.handleIgnoredMessage();
        verifyFailure( handler2 );
    }

    @Test
    void shouldFailHandlerOnIgnoredMessageWhenHandlingReset()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );

        dispatcher.muteAckFailure();
        dispatcher.handleIgnoredMessage();

        verify( handler ).onFailure( any( ClientException.class ) );
    }

    @Test
    void shouldFailHandlerOnIgnoredMessageWhenNoErrorAndNotHandlingReset()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );

        dispatcher.handleIgnoredMessage();

        verify( handler ).onFailure( any( ClientException.class ) );
    }

    @Test
    void shouldDequeAndFailHandlerOnIgnoredWhenErrorHappened()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.queue( handler1 );
        dispatcher.queue( handler2 );
        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        dispatcher.handleIgnoredMessage();

        // ACK_FAILURE handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler1 );
        verifyFailure( handler2 );
    }

    @Test
    void shouldMuteAndUnMuteAckFailure()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        assertFalse( dispatcher.isAckFailureMuted() );

        dispatcher.muteAckFailure();
        assertTrue( dispatcher.isAckFailureMuted() );

        dispatcher.unMuteAckFailure();
        assertFalse( dispatcher.isAckFailureMuted() );
    }

    @Test
    void shouldThrowWhenNoHandlerToHandleRecordMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        assertThrows( IllegalStateException.class, () -> dispatcher.handleRecordMessage( new Value[]{value( 1 ), value( 2 )} ) );
    }

    private static void verifyFailure( ResponseHandler handler )
    {
        ArgumentCaptor<Neo4jException> captor = ArgumentCaptor.forClass( Neo4jException.class );
        verify( handler ).onFailure( captor.capture() );
        assertEquals( FAILURE_CODE, captor.getValue().code() );
        assertEquals( FAILURE_MESSAGE, captor.getValue().getMessage() );
    }

    private static InboundMessageDispatcher newDispatcher()
    {
        return newDispatcher( mock( Channel.class ) );
    }

    private static InboundMessageDispatcher newDispatcher( Channel channel )
    {
        return new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
    }
}
