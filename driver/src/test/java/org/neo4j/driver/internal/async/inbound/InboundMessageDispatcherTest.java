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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelId;
import io.netty.util.Attribute;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.Values.value;

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
        assertThrows( NullPointerException.class, () -> new InboundMessageDispatcher( newChannelMock(), null ) );
    }

    @Test
    void shouldDequeHandlerOnSuccess()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );
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
        dispatcher.enqueue( handler );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        // "RESET after failure" handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler );
        assertEquals( FAILURE_CODE, ((Neo4jException) dispatcher.currentError()).code() );
        assertEquals( FAILURE_MESSAGE, dispatcher.currentError().getMessage() );
    }

    @Test
    void shouldSendResetOnFailure()
    {
        Channel channel = newChannelMock();
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel ).writeAndFlush( eq( RESET ), any() );
    }

    @Test
    void shouldClearFailureOnSuccessOfResetAfterFailure()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
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
        dispatcher.enqueue( handler );
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
    void shouldFailAllHandlersOnChannelError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );
        ResponseHandler handler3 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleChannelError( fatalError );

        InOrder inOrder = inOrder( handler1, handler2, handler3 );
        inOrder.verify( handler1 ).onFailure( fatalError );
        inOrder.verify( handler2 ).onFailure( fatalError );
        inOrder.verify( handler3 ).onFailure( fatalError );
    }

    @Test
    void shouldFailNewHandlerAfterChannelError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleChannelError( fatalError );

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        verify( handler ).onFailure( fatalError );
    }

    @Test
    void shouldAttachChannelErrorOnExistingError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        dispatcher.handleFailureMessage( "Neo.ClientError", "First error!" );
        RuntimeException fatalError = new RuntimeException( "Second Error!" );
        dispatcher.handleChannelError( fatalError );

        verify( handler ).onFailure( argThat(
                error -> error instanceof ClientException && error.getMessage().equals( "First error!" ) &&
                        error.getSuppressed().length == 1 && error.getSuppressed()[0].getMessage().equals( "Second Error!" ) ) );
    }

    @Test
    void shouldDequeHandlerOnIgnored()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );

        dispatcher.enqueue( handler );
        dispatcher.handleIgnoredMessage();

        assertEquals( 0, dispatcher.queuedHandlersCount() );
    }

    @Test
    void shouldFailHandlerOnIgnoredMessageWithExistingError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verifyFailure( handler1 );
        verify( handler2, only() ).canManageAutoRead();

        dispatcher.handleIgnoredMessage();
        verifyFailure( handler2 );
    }

    @Test
    void shouldFailHandlerOnIgnoredMessageWhenNoErrorAndNotHandlingReset()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        dispatcher.handleIgnoredMessage();

        verify( handler ).onFailure( any( ClientException.class ) );
    }

    @Test
    void shouldDequeAndFailHandlerOnIgnoredWhenErrorHappened()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        dispatcher.handleIgnoredMessage();

        // "RESET after failure" handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler1 );
        verifyFailure( handler2 );
    }

    @Test
    void shouldThrowWhenNoHandlerToHandleRecordMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        assertThrows( IllegalStateException.class, () -> dispatcher.handleRecordMessage( new Value[]{value( 1 ), value( 2 )} ) );
    }

    @Test
    void shouldKeepSingleAutoReadManagingHandler()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = newAutoReadManagingResponseHandler();
        ResponseHandler handler2 = newAutoReadManagingResponseHandler();
        ResponseHandler handler3 = newAutoReadManagingResponseHandler();

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );

        InOrder inOrder = inOrder( handler1, handler2, handler3 );
        inOrder.verify( handler1 ).disableAutoReadManagement();
        inOrder.verify( handler2 ).disableAutoReadManagement();
        inOrder.verify( handler3, never() ).disableAutoReadManagement();
    }

    @Test
    void shouldKeepTrackOfAutoReadManagingHandler()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = newAutoReadManagingResponseHandler();
        ResponseHandler handler2 = newAutoReadManagingResponseHandler();

        assertNull( dispatcher.autoReadManagingHandler() );

        dispatcher.enqueue( handler1 );
        assertEquals( handler1, dispatcher.autoReadManagingHandler() );

        dispatcher.enqueue( handler2 );
        assertEquals( handler2, dispatcher.autoReadManagingHandler() );
    }

    @Test
    void shouldForgetAutoReadManagingHandlerWhenItIsRemoved()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );
        ResponseHandler handler3 = newAutoReadManagingResponseHandler();

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );
        assertEquals( handler3, dispatcher.autoReadManagingHandler() );

        dispatcher.handleSuccessMessage( emptyMap() );
        dispatcher.handleSuccessMessage( emptyMap() );
        dispatcher.handleSuccessMessage( emptyMap() );

        assertNull( dispatcher.autoReadManagingHandler() );
    }

    @Test
    void shouldReEnableAutoReadWhenAutoReadManagingHandlerIsRemoved()
    {
        Channel channel = newChannelMock();
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        ResponseHandler handler = newAutoReadManagingResponseHandler();
        dispatcher.enqueue( handler );
        assertEquals( handler, dispatcher.autoReadManagingHandler() );
        verify( handler, never() ).disableAutoReadManagement();
        verify( channel.config(), never() ).setAutoRead( anyBoolean() );

        dispatcher.handleSuccessMessage( emptyMap() );

        assertNull( dispatcher.autoReadManagingHandler() );
        verify( handler ).disableAutoReadManagement();
        verify( channel.config() ).setAutoRead( anyBoolean() );
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
        return newDispatcher( newChannelMock() );
    }

    private static InboundMessageDispatcher newDispatcher( Channel channel )
    {
        return new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
    }

    @SuppressWarnings( "unchecked" )
    private static Channel newChannelMock()
    {
        Channel channel = mock( Channel.class );
        when( channel.id() ).thenReturn( DefaultChannelId.newInstance() );
        ChannelConfig channelConfig = mock( ChannelConfig.class );
        when( channel.config() ).thenReturn( channelConfig );
        Attribute attribute = mock( Attribute.class );
        when( channel.attr( any() ) ).thenReturn( attribute );
        return channel;
    }

    private static ResponseHandler newAutoReadManagingResponseHandler()
    {
        ResponseHandler handler = mock( ResponseHandler.class );
        when( handler.canManageAutoRead() ).thenReturn( true );
        return handler;
    }
}
