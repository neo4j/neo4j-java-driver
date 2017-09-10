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

import io.netty.channel.Channel;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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
    public void shouldDequeHandlerOnFailure()
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
    public void shouldSendAckFailureOnFailure()
    {
        Channel channel = mock( Channel.class );
        InboundMessageDispatcher dispatcher = newDispatcher( channel );

        dispatcher.queue( mock( ResponseHandler.class ) );
        assertEquals( 1, dispatcher.queuedHandlersCount() );

        dispatcher.handleFailureMessage( FAILURE_CODE, FAILURE_MESSAGE );

        verify( channel ).writeAndFlush( ACK_FAILURE );
    }

    @Test
    public void shouldClearFailureOnAckFailureSuccess()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        dispatcher.queue( mock( ResponseHandler.class ) );
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
    public void shouldFailAllHandlersOnFatalError()
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
    public void shouldFailNewHandlerAfterFatalError()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        RuntimeException fatalError = new RuntimeException( "Fatal!" );
        dispatcher.handleFatalError( fatalError );

        ResponseHandler handler = mock( ResponseHandler.class );
        dispatcher.queue( handler );

        verify( handler ).onFailure( fatalError );
    }

    @Test
    public void shouldDequeHandlerOnIgnored()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();
        ResponseHandler handler = mock( ResponseHandler.class );

        dispatcher.queue( handler );
        dispatcher.handleIgnoredMessage();

        assertEquals( 0, dispatcher.queuedHandlersCount() );
        verifyZeroInteractions( handler );
    }

    @Test
    public void shouldDequeAndFailHandlerOnIgnoredWhenErrorHappened()
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
    public void shouldNotSupportInitMessage()
    {
        InboundMessageDispatcher dispatcher = newDispatcher();

        try
        {
            dispatcher.handleInitMessage( "Client", Collections.<String,Value>emptyMap() );
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
            dispatcher.handleRunMessage( "RETURN 1", Collections.<String,Value>emptyMap() );
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
