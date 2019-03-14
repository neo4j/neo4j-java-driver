/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.DefaultBookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
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
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.reactive.cursor.InternalStatementResultCursor;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.util.ServerVersion.v3_5_0;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;

public class BoltProtocolV3Test
{
    protected static final String QUERY = "RETURN $x";
    protected static final Map<String,Value> PARAMS = singletonMap( "x", value( 42 ) );
    protected static final Statement STATEMENT = new Statement( QUERY, value( PARAMS ) );

    protected final BoltProtocol protocol = createProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, Logging.none() );

    private final TransactionConfig txConfig = TransactionConfig.builder()
            .withTimeout( ofSeconds( 12 ) )
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

    protected BoltProtocol createProtocol()
    {
        return BoltProtocolV3.INSTANCE;
    }

    protected Class<? extends MessageFormat> expectedMessageFormatType()
    {
        return MessageFormatV3.class;
    }

    @Test
    void shouldCreateMessageFormat()
    {
        assertThat( protocol.createMessageFormat(), instanceOf( expectedMessageFormatType() ) );
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

        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "server", value( v3_5_0.toString() ) );
        metadata.put( "connection_id", value( "bolt-42" ) );

        messageDispatcher.handleSuccessMessage( metadata );

        assertTrue( promise.isDone() );
        assertTrue( promise.isSuccess() );
    }

    @Test
    void shouldPrepareToCloseChannel()
    {
        protocol.prepareToCloseChannel( channel );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( GoodbyeMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
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
        Connection connection = connectionMock( protocol );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, Bookmarks.empty(), TransactionConfig.empty() );

        verify( connection ).write( new BeginMessage( Bookmarks.empty(), TransactionConfig.empty(), WRITE, ABSENT_DB_NAME ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarks()
    {
        Connection connection = connectionMock( protocol );
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx100" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmarks, TransactionConfig.empty() );

        verify( connection ).writeAndFlush( eq( new BeginMessage( bookmarks, TransactionConfig.empty(), WRITE, ABSENT_DB_NAME ) ), any( BeginTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithConfig()
    {
        Connection connection = connectionMock( protocol );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, Bookmarks.empty(), txConfig );

        verify( connection ).write( new BeginMessage( Bookmarks.empty(), txConfig, WRITE, ABSENT_DB_NAME ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarksAndConfig()
    {
        Connection connection = connectionMock( protocol );
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx4242" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmarks, txConfig );

        verify( connection ).writeAndFlush( eq( new BeginMessage( bookmarks, txConfig, WRITE, ABSENT_DB_NAME ) ), any( BeginTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldCommitTransaction()
    {
        String bookmarkString = "neo4j:bookmark:v1:tx4242";

        Connection connection = connectionMock( protocol );
        when( connection.protocol() ).thenReturn( protocol );
        doAnswer( invocation ->
        {
            ResponseHandler commitHandler = invocation.getArgument( 1 );
            commitHandler.onSuccess( singletonMap( "bookmark", value( bookmarkString ) ) );
            return null;
        } ).when( connection ).writeAndFlush( eq( CommitMessage.COMMIT ), any() );

        CompletionStage<Bookmarks> stage = protocol.commitTransaction( connection );

        verify( connection ).writeAndFlush( eq( CommitMessage.COMMIT ), any( CommitTxResponseHandler.class ) );
        assertEquals( Bookmarks.from( bookmarkString ), await( stage ) );
    }

    @Test
    void shouldRollbackTransaction()
    {
        Connection connection = connectionMock( protocol );

        CompletionStage<Void> stage = protocol.rollbackTransaction( connection );

        verify( connection ).writeAndFlush( eq( RollbackMessage.ROLLBACK ), any( RollbackTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionWithoutWaitingForRunResponse( AccessMode mode ) throws Exception
    {
        testRunWithoutWaitingForRunResponse( true, TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitWithConfigTransactionWithoutWaitingForRunResponse( AccessMode mode ) throws Exception
    {
        testRunWithoutWaitingForRunResponse( true, txConfig, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionAndWaitForSuccessRunResponse( AccessMode mode ) throws Exception
    {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmarks.empty(), TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForSuccessRunResponse( AccessMode mode ) throws Exception
    {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmarks.from( "neo4j:bookmark:v1:tx65" ), txConfig, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmarks.empty(), TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmarks.from( "neo4j:bookmark:v1:tx163" ), txConfig, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInExplicitTransactionWithoutWaitingForRunResponse( AccessMode mode ) throws Exception
    {
        testRunWithoutWaitingForRunResponse( false, TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInExplicitTransactionAndWaitForSuccessRunResponse( AccessMode mode ) throws Exception
    {
        testRunInExplicitTransactionAndWaitForRunResponse( true, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInExplicitTransactionAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testRunInExplicitTransactionAndWaitForRunResponse( false, mode );
    }

    @Test
    void databaseNameInBeginTransaction()
    {
        testDatabaseNameSupport( false );
    }

    @Test
    void databaseNameForAutoCommitTransactions()
    {
        testDatabaseNameSupport( true );
    }

    protected void testDatabaseNameSupport( boolean autoCommitTx )
    {
        ClientException e;
        if ( autoCommitTx )
        {
            e = assertThrows( ClientException.class,
                    () -> protocol.runInAutoCommitTransaction( connectionMock( "foo", protocol ), new Statement( "RETURN 1" ), BookmarksHolder.NO_OP,
                            TransactionConfig.empty(), true ) );
        }
        else
        {
            CompletionStage<Void> txStage = protocol.beginTransaction( connectionMock( "foo", protocol ), Bookmarks.empty(), TransactionConfig.empty() );
            e = assertThrows( ClientException.class, () -> await( txStage ) );
        }

        assertThat( e.getMessage(), startsWith( "Database name parameter for selecting database is not supported" ) );
    }

    protected void testRunInExplicitTransactionAndWaitForRunResponse( boolean success, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true ).asyncResult().toCompletableFuture();

        ResponseHandler runResponseHandler = verifyRunInvoked( connection, false, Bookmarks.empty(), TransactionConfig.empty(), mode ).runHandler;
        assertFalse( cursorFuture.isDone() );

        if ( success )
        {
            runResponseHandler.onSuccess( emptyMap() );
        }
        else
        {
            // When responded with a failure
            runResponseHandler.onFailure( new RuntimeException() );
        }

        // Then
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    protected void testRunWithoutWaitingForRunResponse( boolean autoCommitTx, TransactionConfig config, AccessMode mode ) throws Exception
    {
        Connection connection = connectionMock( mode, protocol );
        Bookmarks initialBookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx987" );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( autoCommitTx )
        {
            BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( initialBookmarks );
            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, false ).asyncResult();
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false ).asyncResult();
        }
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );

        if ( autoCommitTx )
        {
            verifyRunInvoked( connection, autoCommitTx, initialBookmarks, config, mode );
        }
        else
        {
            verifyRunInvoked( connection, autoCommitTx, Bookmarks.empty(), config, mode );
        }
    }

    protected void testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        Connection connection = connectionMock( mode, protocol );
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( bookmarks );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).asyncResult().toCompletableFuture();
        assertFalse( cursorFuture.isDone() );

        ResponseHandlers handlers = verifyRunInvoked( connection, true, bookmarks, config, mode );

        String newBookmarkValue = "neo4j:bookmark:v1:tx98765";
        handlers.runHandler.onSuccess( emptyMap() );
        handlers.pullAllHandler.onSuccess( singletonMap( "bookmark", value( newBookmarkValue ) ) );
        assertEquals( Bookmarks.from( newBookmarkValue ), bookmarksHolder.getBookmarks() );

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    protected void testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        Connection connection = connectionMock( mode, protocol );
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( bookmarks );

        CompletableFuture<InternalStatementResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).asyncResult().toCompletableFuture();
        assertFalse( cursorFuture.isDone() );

        ResponseHandler runResponseHandler = verifyRunInvoked( connection, true, bookmarks, config, mode ).runHandler;
        runResponseHandler.onFailure( new RuntimeException() );
        assertEquals( bookmarks, bookmarksHolder.getBookmarks() );

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private static Map<String,Value> dummyAuthToken()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "username", value( "hello" ) );
        authToken.put( "password", value( "world" ) );
        return authToken;
    }

    private static ResponseHandlers verifyRunInvoked( Connection connection, boolean session, Bookmarks bookmarks, TransactionConfig config, AccessMode mode )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullAllHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        RunWithMetadataMessage expectedMessage;
        if ( session )
        {
            expectedMessage = RunWithMetadataMessage.autoCommitTxRunMessage( STATEMENT, bookmarks, config, mode, ABSENT_DB_NAME );
        }
        else
        {
            expectedMessage = RunWithMetadataMessage.explicitTxRunMessage( STATEMENT );
        }

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

        return new ResponseHandlers( runHandlerCaptor.getValue(), pullAllHandlerCaptor.getValue() );
    }

    private static class ResponseHandlers
    {
        final ResponseHandler runHandler;
        final ResponseHandler pullAllHandler;

        ResponseHandlers( ResponseHandler runHandler, ResponseHandler pullAllHandler )
        {
            this.runHandler = runHandler;
            this.pullAllHandler = pullAllHandler;
        }
    }
}
