/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.v4;

import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DefaultBookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3Test;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;

class BoltProtocolV4Test extends BoltProtocolV3Test
{
    @Override
    protected BoltProtocol createProtocol()
    {
        return BoltProtocolV4.INSTANCE;
    }

    @Override
    protected Class<? extends MessageFormat> expectedMessageFormatType()
    {
        return MessageFormatV4.class;
    }

    @Override
    protected void testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmark bookmark, TransactionConfig config, AccessMode mode ) throws Exception
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

    @Override
    protected void testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmark bookmark, TransactionConfig config, AccessMode mode ) throws Exception
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

    @Override
    protected void testRunInUnmanagedTransactionAndWaitForRunResponse(boolean success, AccessMode mode ) throws Exception
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

    @Override
    protected void testRunWithoutWaitingForRunResponse( boolean autoCommitTx, TransactionConfig config, AccessMode mode ) throws Exception
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

    @Override
    protected void testDatabaseNameSupport( boolean autoCommitTx )
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
        return verifyRunInvoked( connection, RunWithMetadataMessage.unmanagedTxRunMessage(QUERY) );
    }

    private ResponseHandler verifySessionRunInvoked( Connection connection, Bookmark bookmark, TransactionConfig config, AccessMode mode, DatabaseName databaseName )
    {
        RunWithMetadataMessage runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(QUERY, config, databaseName, mode, bookmark );
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

        if( bookmark.isEmpty() )
        {
            verify( connection ).write( eq( beginMessage ), eq( NoOpResponseHandler.INSTANCE ) );
        }
        else
        {
            verify( connection ).write( eq( beginMessage ), beginHandlerCaptor.capture() );
            assertThat( beginHandlerCaptor.getValue(), instanceOf( BeginTxResponseHandler.class ) );
        }
    }
}
