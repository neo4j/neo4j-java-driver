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
package org.neo4j.driver.internal.messaging.v41;

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

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DefaultBookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;

public final class BoltProtocolV41Test
{
    protected static final String QUERY_TEXT = "RETURN $x";
    protected static final Map<String,Value> PARAMS = singletonMap( "x", value( 42 ) );
    protected static final Query QUERY = new Query( QUERY_TEXT, value( PARAMS ) );

    protected final BoltProtocol protocol = createProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, Logging.none() );

    private final TransactionConfig txConfig = TransactionConfig.builder()
                                                                .withTimeout( ofSeconds( 12 ) )
                                                                .withMetadata( singletonMap( "key", value( 42 ) ) )
                                                                .build();

    private BoltProtocol createProtocol()
    {
        return BoltProtocolV41.INSTANCE;
    }

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
        assertThat( protocol.createMessageFormat(), instanceOf( expectedMessageFormatType() ) );
    }

    @Test
    void shouldInitializeChannel()
    {
        ChannelPromise promise = channel.newPromise();

        protocol.initializeChannel( "MyDriver/0.0.1", dummyAuthToken(), RoutingContext.EMPTY, promise );

        assertThat( channel.outboundMessages(), hasSize( 1 ) );
        assertThat( channel.outboundMessages().poll(), instanceOf( HelloMessage.class ) );
        assertEquals( 1, messageDispatcher.queuedHandlersCount() );
        assertFalse( promise.isDone() );

        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "server", value( anyServerVersion().toString() ) );
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

        protocol.initializeChannel( "MyDriver/2.2.1", dummyAuthToken(), RoutingContext.EMPTY, promise );

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

        CompletionStage<Void> stage = protocol.beginTransaction( connection, InternalBookmark.empty(), TransactionConfig.empty() );

        verify( connection )
                .write( new BeginMessage( InternalBookmark.empty(), TransactionConfig.empty(), defaultDatabase(), WRITE ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarks()
    {
        Connection connection = connectionMock( protocol );
        Bookmark bookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx100" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmark, TransactionConfig.empty() );

        verify( connection )
                .writeAndFlush( eq( new BeginMessage( bookmark, TransactionConfig.empty(), defaultDatabase(), WRITE ) ), any( BeginTxResponseHandler.class ) );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithConfig()
    {
        Connection connection = connectionMock( protocol );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, InternalBookmark.empty(), txConfig );

        verify( connection ).write( new BeginMessage( InternalBookmark.empty(), txConfig, defaultDatabase(), WRITE ), NoOpResponseHandler.INSTANCE );
        assertNull( await( stage ) );
    }

    @Test
    void shouldBeginTransactionWithBookmarksAndConfig()
    {
        Connection connection = connectionMock( protocol );
        Bookmark bookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx4242" );

        CompletionStage<Void> stage = protocol.beginTransaction( connection, bookmark, txConfig );

        verify( connection ).writeAndFlush( eq( new BeginMessage( bookmark, txConfig, defaultDatabase(), WRITE ) ), any( BeginTxResponseHandler.class ) );
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

        CompletionStage<Bookmark> stage = protocol.commitTransaction( connection );

        verify( connection ).writeAndFlush( eq( CommitMessage.COMMIT ), any( CommitTxResponseHandler.class ) );
        assertEquals( InternalBookmark.parse( bookmarkString ), await( stage ) );
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
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse( InternalBookmark.empty(), TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForSuccessRunResponse( AccessMode mode ) throws Exception
    {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse( InternalBookmark.parse( "neo4j:bookmark:v1:tx65" ), txConfig, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testFailedRunInAutoCommitTxWithWaitingForResponse( InternalBookmark.empty(), TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testFailedRunInAutoCommitTxWithWaitingForResponse( InternalBookmark.parse( "neo4j:bookmark:v1:tx163" ), txConfig, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInUnmanagedTransactionWithoutWaitingForRunResponse( AccessMode mode ) throws Exception
    {
        testRunWithoutWaitingForRunResponse( false, TransactionConfig.empty(), mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInUnmanagedTransactionAndWaitForSuccessRunResponse( AccessMode mode ) throws Exception
    {
        testRunInUnmanagedTransactionAndWaitForRunResponse( true, mode );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldRunInUnmanagedTransactionAndWaitForFailureRunResponse( AccessMode mode ) throws Exception
    {
        testRunInUnmanagedTransactionAndWaitForRunResponse( false, mode );
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

    @Test
    void shouldSupportDatabaseNameInBeginTransaction()
    {
        CompletionStage<Void> txStage = protocol.beginTransaction( connectionMock( "foo", protocol ), InternalBookmark.empty(), TransactionConfig.empty() );

        assertDoesNotThrow( () -> await( txStage ) );
    }

    @Test
    void shouldNotSupportDatabaseNameForAutoCommitTransactions()
    {
        assertDoesNotThrow(
                () -> protocol.runInAutoCommitTransaction( connectionMock( "foo", protocol ),
                                                           new Query( "RETURN 1" ), BookmarkHolder.NO_OP, TransactionConfig.empty(), true,
                                                           UNLIMITED_FETCH_SIZE ) );
    }

    private Class<? extends MessageFormat> expectedMessageFormatType()
    {
        return MessageFormatV4.class;
    }

    private void testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmark bookmark, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( bookmark );

        CompletableFuture<AsyncResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, QUERY, bookmarkHolder, config, true, UNLIMITED_FETCH_SIZE )
                        .asyncResult()
                        .toCompletableFuture();

        ResponseHandler runHandler = verifySessionRunInvoked( connection, bookmark, config, mode, defaultDatabase() );
        assertFalse( cursorFuture.isDone() );

        // When I response to Run message with a failure
        runHandler.onFailure( new RuntimeException() );

        // Then
        assertEquals( bookmark, bookmarkHolder.getBookmark() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private void testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmark bookmark, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( bookmark );

        CompletableFuture<AsyncResultCursor> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, QUERY, bookmarkHolder, config, true, UNLIMITED_FETCH_SIZE )
                        .asyncResult()
                        .toCompletableFuture();

        ResponseHandler runHandler = verifySessionRunInvoked( connection, bookmark, config, mode, defaultDatabase() );
        assertFalse( cursorFuture.isDone() );

        // When I response to the run message
        runHandler.onSuccess( emptyMap() );

        // Then
        assertEquals( bookmark, bookmarkHolder.getBookmark() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private void testRunInUnmanagedTransactionAndWaitForRunResponse( boolean success, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );

        CompletableFuture<AsyncResultCursor> cursorFuture =
                protocol.runInUnmanagedTransaction( connection, QUERY, mock( UnmanagedTransaction.class ), true, UNLIMITED_FETCH_SIZE )
                        .asyncResult()
                        .toCompletableFuture();

        ResponseHandler runHandler = verifyTxRunInvoked( connection );
        assertFalse( cursorFuture.isDone() );

        if ( success )
        {
            runHandler.onSuccess( emptyMap() );
        }
        else
        {
            // When responded with a failure
            runHandler.onFailure( new RuntimeException() );
        }

        // Then
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private void testRunWithoutWaitingForRunResponse( boolean autoCommitTx, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode, protocol );
        Bookmark initialBookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx987" );

        CompletionStage<AsyncResultCursor> cursorStage;
        if ( autoCommitTx )
        {
            BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( initialBookmark );
            cursorStage = protocol.runInAutoCommitTransaction( connection, QUERY, bookmarkHolder, config, false, UNLIMITED_FETCH_SIZE )
                                  .asyncResult();
        }
        else
        {
            cursorStage = protocol.runInUnmanagedTransaction( connection, QUERY, mock( UnmanagedTransaction.class ), false, UNLIMITED_FETCH_SIZE )
                                  .asyncResult();
        }

        // When I complete it immediately without waiting for any responses to run message
        CompletableFuture<AsyncResultCursor> cursorFuture = cursorStage.toCompletableFuture();
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );

        // Then
        if ( autoCommitTx )
        {
            verifySessionRunInvoked( connection, initialBookmark, config, mode, defaultDatabase() );
        }
        else
        {
            verifyTxRunInvoked( connection );
        }
    }

    private void testDatabaseNameSupport( boolean autoCommitTx )
    {
        Connection connection = connectionMock( "foo", protocol );
        if ( autoCommitTx )
        {
            ResultCursorFactory factory =
                    protocol.runInAutoCommitTransaction( connection, QUERY, BookmarkHolder.NO_OP, TransactionConfig.empty(), false, UNLIMITED_FETCH_SIZE );
            await( factory.asyncResult() );
            verifySessionRunInvoked( connection, InternalBookmark.empty(), TransactionConfig.empty(), AccessMode.WRITE, database( "foo" ) );
        }
        else
        {
            CompletionStage<Void> txStage = protocol.beginTransaction( connection, InternalBookmark.empty(), TransactionConfig.empty() );
            await( txStage );
            verifyBeginInvoked( connection, InternalBookmark.empty(), TransactionConfig.empty(), AccessMode.WRITE, database( "foo" ) );
        }
    }

    private ResponseHandler verifyTxRunInvoked( Connection connection )
    {
        return verifyRunInvoked( connection, RunWithMetadataMessage.unmanagedTxRunMessage( QUERY ) );
    }

    private ResponseHandler verifySessionRunInvoked( Connection connection, Bookmark bookmark, TransactionConfig config, AccessMode mode,
                                                     DatabaseName databaseName )
    {
        RunWithMetadataMessage runMessage = RunWithMetadataMessage.autoCommitTxRunMessage( QUERY, config, databaseName, mode, bookmark );
        return verifyRunInvoked( connection, runMessage );
    }

    private ResponseHandler verifyRunInvoked( Connection connection, RunWithMetadataMessage runMessage )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        verify( connection ).write( eq( runMessage ), runHandlerCaptor.capture() );
        verify( connection ).writeAndFlush( any( PullMessage.class ), pullHandlerCaptor.capture() );

        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );
        assertThat( pullHandlerCaptor.getValue(), instanceOf( PullAllResponseHandler.class ) );

        return runHandlerCaptor.getValue();
    }

    private void verifyBeginInvoked( Connection connection, Bookmark bookmark, TransactionConfig config, AccessMode mode, DatabaseName databaseName )
    {
        ArgumentCaptor<ResponseHandler> beginHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        BeginMessage beginMessage = new BeginMessage( bookmark, config, databaseName, mode );

        if ( bookmark.isEmpty() )
        {
            verify( connection ).write( eq( beginMessage ), eq( NoOpResponseHandler.INSTANCE ) );
        }
        else
        {
            verify( connection ).write( eq( beginMessage ), beginHandlerCaptor.capture() );
            assertThat( beginHandlerCaptor.getValue(), instanceOf( BeginTxResponseHandler.class ) );
        }
    }

    private static InternalAuthToken dummyAuthToken()
    {
        return (InternalAuthToken) AuthTokens.basic( "hello", "world" );
    }
}

