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
import io.netty.channel.ChannelConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.spi.AutoReadManagingResponseHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.v1.Values.value;

public class InboundMessageDispatcherTest
{
    private static final String FAILURE_CODE = "Neo.ClientError.Security.Unauthorized";
    private static final String FAILURE_MESSAGE = "Error Message";

    @Test
    public void shouldFailWhenCreatedWithNullChannel()
    {
        try
        {
            new InboundMessageDispatcher( null, DEV_NULL_LOGGING );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void shouldFailWhenCreatedWithNullLogging()
    {
        try
        {
            new InboundMessageDispatcher( mock( Channel.class ), null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void shouldDequeHandlerOnSuccess()
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
    public void shouldDequeHandlerOnFailure()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        // ACK_FAILURE handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler );
        assertEquals( FAILURE_CODE, ((Neo4jException) dispatcher.currentError()).code() );
        assertEquals( FAILURE_MESSAGE, dispatcher.currentError().getMessage() );
    }

    @Test
    public void shouldSendAckFailureOnFailure()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    public void shouldNotSendAckFailureOnFailureWhenMuted()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );
        dispatcher.muteAckFailure();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel, never() ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    public void shouldUnMuteAckFailureWhenNotMuted()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.unMuteAckFailure();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    public void shouldSendAckFailureAfterUnMute()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );
        dispatcher.muteAckFailure();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel, never() ).writeAndFlush( eq( ACK_FAILURE ), any() );

        dispatcher.unMuteAckFailure();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verify( channel, times( 1 ) ).writeAndFlush( eq( ACK_FAILURE ), any() );
    }

    @Test
    public void shouldClearFailureOnAckFailureSuccess()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        dispatcher.enqueue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        dispatcher.handleSuccessMessage( Collections.<String,Value>emptyMap() );

        assertNull( dispatcher.currentError() );
    }

    @Test
    public void shouldPeekHandlerOnRecord()
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
    public void shouldFailAllHandlersOnFatalError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );
        ResponseHandler handler3 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleFatalError( fatalError );

        InOrder inOrder = inOrder( handler1, handler2, handler3 );
        inOrder.verify( handler1 ).onFailure( fatalError );
        inOrder.verify( handler2 ).onFailure( fatalError );
        inOrder.verify( handler3 ).onFailure( fatalError );
    }

    @Test
    public void shouldFailNewHandlerAfterFatalError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleFatalError( fatalError );

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        verify( handler ).onFailure( fatalError );
    }

    @Test
    public void shouldDequeHandlerOnIgnored()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );

        dispatcher.enqueue( handler );
        dispatcher.handleIgnoredMessage();

        assertEquals( 0, dispatcher.queuedHandlersCount() );
    }

    @Test
    public void shouldFailHandlerOnIgnoredMessageWithExistingError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        verifyFailure( handler1 );
        verifyZeroInteractions( handler2 );

        dispatcher.handleIgnoredMessage();
        verifyFailure( handler2 );
    }

    @Test
    public void shouldFailHandlerOnIgnoredMessageWhenHandlingReset()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        dispatcher.muteAckFailure();
        dispatcher.handleIgnoredMessage();

        verify( handler ).onFailure( any( ClientException.class ) );
    }

    @Test
    public void shouldFailHandlerOnIgnoredMessageWhenNoErrorAndNotHandlingReset()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.enqueue( handler );

        dispatcher.handleIgnoredMessage();

        verify( handler ).onFailure( any( ClientException.class ) );
    }

    @Test
    public void shouldDequeAndFailHandlerOnIgnoredWhenErrorHappened()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );
        dispatcher.handleIgnoredMessage();

        // ACK_FAILURE handler should remain queued
        assertEquals( 1, dispatcher.queuedHandlersCount() );
        verifyFailure( handler1 );
        verifyFailure( handler2 );
    }

    @Test
    public void shouldNotSupportInitMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleInitMessage( "Client", emptyMap() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldNotSupportRunMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleRunMessage( "RETURN 1", emptyMap() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldNotSupportPullAllMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handlePullAllMessage();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldNotSupportDiscardAllMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleDiscardAllMessage();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldNotSupportResetMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleResetMessage();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldNotSupportAckFailureMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleAckFailureMessage();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void shouldMuteAndUnMuteAckFailure()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        assertFalse( dispatcher.isAckFailureMuted() );

        dispatcher.muteAckFailure();
        assertTrue( dispatcher.isAckFailureMuted() );

        dispatcher.unMuteAckFailure();
        assertFalse( dispatcher.isAckFailureMuted() );
    }

    @Test
    public void shouldKeepSingleAutoReadManagingHandler()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        AutoReadManagingResponseHandler handler1 = mock( AutoReadManagingResponseHandler.class );
        AutoReadManagingResponseHandler handler2 = mock( AutoReadManagingResponseHandler.class );
        AutoReadManagingResponseHandler handler3 = mock( AutoReadManagingResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );

        InOrder inOrder = inOrder( handler1, handler2, handler3 );
        inOrder.verify( handler1 ).disableAutoReadManagement();
        inOrder.verify( handler2 ).disableAutoReadManagement();
        inOrder.verify( handler3, never() ).disableAutoReadManagement();
    }

    @Test
    public void shouldKeepTrackOfAutoReadManagingHandler()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        AutoReadManagingResponseHandler handler1 = mock( AutoReadManagingResponseHandler.class );
        AutoReadManagingResponseHandler handler2 = mock( AutoReadManagingResponseHandler.class );

        assertNull( dispatcher.autoReadManagingHandler() );

        dispatcher.enqueue( handler1 );
        assertEquals( handler1, dispatcher.autoReadManagingHandler() );

        dispatcher.enqueue( handler2 );
        assertEquals( handler2, dispatcher.autoReadManagingHandler() );
    }

    @Test
    public void shouldForgetAutoReadManagingHandlerWhenItIsRemoved()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        ResponseHandler handler1 = mock( ResponseHandler.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );
        AutoReadManagingResponseHandler handler3 = mock( AutoReadManagingResponseHandler.class );

        dispatcher.enqueue( handler1 );
        dispatcher.enqueue( handler2 );
        dispatcher.enqueue( handler3 );
        assertEquals( handler3, dispatcher.autoReadManagingHandler() );

        dispatcher.handleSuccessMessage( emptyMap() );
        dispatcher.handleSuccessMessage( emptyMap() );
        dispatcher.handleSuccessMessage( emptyMap() );

        assertNull( dispatcher.autoReadManagingHandler() );
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

    private static Channel newChannelMock()
    {
        Channel channel = mock( Channel.class );
        ChannelConfig channelConfig = mock( ChannelConfig.class );
        when( channel.config() ).thenReturn( channelConfig );
        return channel;
    }
}
