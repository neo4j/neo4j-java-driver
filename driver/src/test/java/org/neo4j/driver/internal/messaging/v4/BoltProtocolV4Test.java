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
package org.neo4j.driver.internal.messaging.v4;

import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.SimpleBookmarksHolder;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3Test;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.react.result.StatementResultCursorFactory;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.TransactionConfig;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;

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
    protected void testFailedRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode );
        BookmarksHolder bookmarksHolder = new SimpleBookmarksHolder( bookmarks );

        CompletableFuture<StatementResultCursorFactory> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).toCompletableFuture();

        ResponseHandler runHandler = verifyRunInvoked( connection, bookmarks, config, mode  );
        assertFalse( cursorFuture.isDone() );

        // When I response to Run message with a failure
        runHandler.onFailure( new RuntimeException() );

        // Then
        assertEquals( bookmarks, bookmarksHolder.getBookmarks() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    @Override
    protected void testSuccessfulRunInAutoCommitTxWithWaitingForResponse( Bookmarks bookmarks, TransactionConfig config, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode );
        BookmarksHolder bookmarksHolder = new SimpleBookmarksHolder( bookmarks );

        CompletableFuture<StatementResultCursorFactory> cursorFuture =
                protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, true ).toCompletableFuture();

        ResponseHandler runHandler = verifyRunInvoked( connection, bookmarks, config, mode );
        assertFalse( cursorFuture.isDone() );

        // When I response to the run message
        runHandler.onSuccess( emptyMap() );

        // Then
        assertEquals( bookmarks, bookmarksHolder.getBookmarks() );
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    protected void testRunInExplicitTransactionAndWaitForRunResponse( boolean success, AccessMode mode ) throws Exception
    {
        // Given
        Connection connection = connectionMock( mode );

        CompletableFuture<StatementResultCursorFactory> cursorFuture =
                protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true ).toCompletableFuture();

        ResponseHandler runHandler = verifyRunInvoked( connection, Bookmarks.empty(), TransactionConfig.empty(), mode );
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
        Connection connection = connectionMock( mode );
        Bookmarks initialBookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx987" );

        CompletionStage<StatementResultCursorFactory> cursorStage;
        if ( autoCommitTx )
        {
            BookmarksHolder bookmarksHolder = new SimpleBookmarksHolder( initialBookmarks );
            cursorStage = protocol.runInAutoCommitTransaction( connection, STATEMENT, bookmarksHolder, config, false );
        }
        else
        {
            cursorStage = protocol.runInExplicitTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false );
        }

        // When I complete it immediately without waiting for any responses to run message
        CompletableFuture<StatementResultCursorFactory> cursorFuture = cursorStage.toCompletableFuture();
        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );

        // Then
        if ( autoCommitTx )
        {
            verifyRunInvoked( connection, initialBookmarks, config, mode );
        }
        else
        {
            verifyRunInvoked( connection, Bookmarks.empty(), config, mode );
        }
    }

    private ResponseHandler verifyRunInvoked( Connection connection, Bookmarks bookmarks, TransactionConfig config, AccessMode mode )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        RunWithMetadataMessage runMessage = new RunWithMetadataMessage( QUERY, PARAMS, bookmarks, config, mode );

        verify( connection ).writeAndFlush( eq( runMessage ), runHandlerCaptor.capture() );
        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );

        return runHandlerCaptor.getValue();
    }
}
