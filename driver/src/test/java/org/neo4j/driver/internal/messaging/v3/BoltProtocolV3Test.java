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
package org.neo4j.driver.internal.messaging.v3;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.InternalStatementResultCursor;
import org.neo4j.driver.internal.async.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.util.ServerVersion.v3_5_0;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;

class BoltProtocolV3Test
{
    private static final String QUERY = "RETURN $x";
    private static final Map<String,Value> PARAMS = singletonMap( "x", value( 42 ) );
    private static final Statement STATEMENT = new Statement( QUERY, value( PARAMS ) );

    private final BoltProtocol protocol = BoltProtocolV3.INSTANCE;
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, Logging.none() );

    private final TransactionConfig txConfig = TransactionConfig.builder()
            .withTimeout( Duration.ofSeconds( 12 ) )
            .withMetadata( singletonMap( "key", value( 42 ) ) )
            .build();

    @BeforeEach
    void beforeEach()
    {
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );
    }

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldCreateMessageFormat()
    {
        assertThat( protocol.createMessageFormat(), instanceOf( MessageFormatV3.class ) );
    }

    @Test
    void shouldInitializeChannel()
    {
        ChannelPromise promise = channel.newPromise();

        protocol.initializeChannel( "MyDriver/0.0.1", dummyAuthToken(), promise );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( HelloMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
        assertFalse( promise.isDone() );

        messageDispatcher.handleSuccessMessage( singletonMap( "server", value( v3_5_0.toString() ) ) );

        assertTrue( promise.isDone() );
        assertTrue( promise.isSuccess() );
    }

    @Test
    void shouldFailToInitializeChannelWhenErrorIsReceived()
    {
        ChannelPromise promise = channel.newPromise();

        protocol.initializeChannel( "MyDriver/2.2.1", dummyAuthToken(), promise );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( HelloMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
        assertFalse( promise.isDone() );

        messageDispatcher.handleFailureMessage( "Neo.TransientError.General.DatabaseUnavailable", "Error!" );

        assertTrue( promise.isDone() );
        assertFalse( promise.isSuccess() );
    }

    @Test
    void shouldBeginTransactionWithoutBookmark()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.beginTransaction( connection, Bookmarks.empty(), TransactionConfig.empty() );

        verify( connection ).write( new BeginMessage( Bookmarks.empty(), TransactionConfig.empty() ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarks()
    {
        Connection connection = connectionMock();
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx100" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmarks, TransactionConfig.empty() );

        verify( connection ).writeAndFlush( eq( new BeginMessage( bookmarks, TransactionConfig.empty() ) ), any( BeginTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithConfig()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.beginTransaction( connection, Bookmarks.empty(), txConfig );

        verify( connection ).write( new BeginMessage( Bookmarks.empty(), txConfig ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarksAndConfig()
    {
        Connection connection = connectionMock();
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx4242" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmarks, txConfig );

        verify( connection ).writeAndFlush( eq( new BeginMessage( bookmarks, txConfig ) ), any( BeginTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldCommitTransaction()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.commitTransaction( connection, mock( ExplicitTransaction.class ) );

        verify( connection ).writeAndFlush( eq( CommitMessage.COMMIT ), any( CommitTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldRollbackTransaction()
    {
        Connection connection = connectionMock();

        CompletionStage<Void> stage = protocol.rollbackTransaction( connection );

        verify( connection ).writeAndFlush( eq( RollbackMessage.ROLLBACK ), any( RollbackTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldRunInAutoCommitTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testRunWithoutWaitingForRunResponse( true, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInAutoCommitWithConfigTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testRunWithoutWaitingForRunResponse( true, txConfig );
    }

    @Test
    void shouldRunInAutoCommitTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( true, true, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInAutoCommitWithConfigTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( true, true, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( false, true, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testRunWithoutWaitingForRunResponse( false, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( true, false, TransactionConfig.empty() );
    }

    @Test
    void shouldRunInTransactionAndWaitForFailureRunResponse() throws Exception
    {
        testRunWithWaitingForResponse( false, false, TransactionConfig.empty() );
    }

    private void testRunWithoutWaitingForRunResponse( boolean autoCommitTx, TransactionConfig config ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( autoCommitTx )
        {

            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, Bookmarks.empty(), config, false );
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false );
        }
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
        verifyRunInvoked( connection, autoCommitTx, config );
    }

    private void testRunWithWaitingForResponse( boolean success, boolean session, TransactionConfig config ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( session )
        {
            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, Bookmarks.empty(), TransactionConfig.empty(), true );
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true );
        }
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertFalse( cursorFuture.isDone() );
        ResponseHandler runResponseHandler = verifyRunInvoked( connection, session, config );

        if ( success )
        {
            runResponseHandler.onSuccess( emptyMap() );
        }
        else
        {
            runResponseHandler.onFailure( new RuntimeException() );
        }

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private static ResponseHandler verifyRunInvoked( Connection connection, boolean session, TransactionConfig config )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullAllHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        RunWithMetadataMessage expectedMessage = new RunWithMetadataMessage( QUERY, PARAMS, Bookmarks.empty(), config );

        verify( connection ).writeAndFlush( eq( expectedMessage ), runHandlerCaptor.capture(),
                eq( PullAllMessage.PULL_ALL ), pullAllHandlerCaptor.capture() );

        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );

        if ( session )
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( SessionPullAllResponseHandler.class ) );
        }
        else
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( TransactionPullAllResponseHandler.class ) );
        }

        return runHandlerCaptor.getValue();
    }

    private static Map<String,Value> dummyAuthToken()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "username", value( "hello" ) );
        authToken.put( "password", value( "world" ) );
        return authToken;
    }
}
