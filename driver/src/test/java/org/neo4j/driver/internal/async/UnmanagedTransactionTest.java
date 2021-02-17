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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.util.function.Consumer;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.DefaultBookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.beginMessage;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunRx;
import static org.neo4j.driver.util.TestUtil.verifyBeginTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyRunRx;

class UnmanagedTransactionTest
{
    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldFlushOnRunAsync( boolean waitForResponse )
    {
        // Given
        Connection connection = connectionMock( BoltProtocolV4.INSTANCE );
        UnmanagedTransaction tx = beginTx( connection );
        setupSuccessfulRunAndPull( connection );

        // When
        await( tx.runAsync( new Query( "RETURN 1" ), waitForResponse ) );

        // Then
        verifyRunAndPull( connection, "RETURN 1" );
    }

    @Test
    void shouldFlushOnRunRx()
    {
        // Given
        Connection connection = connectionMock( BoltProtocolV4.INSTANCE );
        UnmanagedTransaction tx = beginTx( connection );
        setupSuccessfulRunRx( connection );

        // When
        await( tx.runRx( new Query( "RETURN 1" ) ) );

        // Then
        verifyRunRx( connection, "RETURN 1" );
    }

    @Test
    void shouldRollbackOnImplicitFailure()
    {
        // Given
        Connection connection = connectionMock();
        UnmanagedTransaction tx = beginTx( connection );

        // When
        await( tx.closeAsync() );

        // Then
        InOrder order = inOrder( connection );
        verifyBeginTx( connection );
        verifyRollbackTx( connection );
        order.verify( connection ).release();
    }

    @Test
    void shouldOnlyQueueMessagesWhenNoBookmarkGiven()
    {
        Connection connection = connectionMock();

        beginTx( connection, InternalBookmark.empty() );

        verifyBeginTx( connection );
        verify( connection, never() ).writeAndFlush( any(), any(), any(), any() );
    }

    @Test
    void shouldFlushWhenBookmarkGiven()
    {
        Bookmark bookmark = InternalBookmark.parse( "hi, I'm bookmark" );
        Connection connection = connectionMock();

        beginTx( connection, bookmark );

        verifyBeginTx( connection, bookmark );
        verify( connection, never() ).write( any(), any(), any(), any() );
    }

    @Test
    void shouldBeOpenAfterConstruction()
    {
        UnmanagedTransaction tx = beginTx( connectionMock() );

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedAsTerminated()
    {
        UnmanagedTransaction tx = beginTx( connectionMock() );

        tx.markTerminated( null );

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedTerminatedAndClosed()
    {
        UnmanagedTransaction tx = beginTx( connectionMock() );

        tx.markTerminated( null );
        await( tx.closeAsync() );

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenBeginFails()
    {
        RuntimeException error = new RuntimeException( "Wrong bookmark!" );
        Connection connection = connectionWithBegin( handler -> handler.onFailure( error ) );
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );

        Bookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        TransactionConfig txConfig = TransactionConfig.empty();

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( tx.beginAsync( bookmark, txConfig ) ) );

        assertEquals( error, e );
        verify( connection ).release();
    }

    @Test
    void shouldNotReleaseConnectionWhenBeginSucceeds()
    {
        Connection connection = connectionWithBegin( handler -> handler.onSuccess( emptyMap() ) );
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );

        Bookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        TransactionConfig txConfig = TransactionConfig.empty();

        await( tx.beginAsync( bookmark, txConfig ) );

        verify( connection, never() ).release();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndCommitted()
    {
        Connection connection = connectionMock();
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );

        tx.markTerminated(  null  );

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );

        assertFalse( tx.isOpen() );
        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndRolledBack()
    {
        Connection connection = connectionMock();
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );

        tx.markTerminated( null );
        await( tx.rollbackAsync() );

        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionWhenClose() throws Throwable
    {
        Connection connection = connectionMock();
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );

        await( tx.closeAsync() );

        verify( connection ).release();
    }

    private static UnmanagedTransaction beginTx(Connection connection )
    {
        return beginTx( connection, InternalBookmark.empty() );
    }

    private static UnmanagedTransaction beginTx(Connection connection, Bookmark initialBookmark )
    {
        UnmanagedTransaction tx = new UnmanagedTransaction( connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE );
        return await( tx.beginAsync( initialBookmark, TransactionConfig.empty() ) );
    }

    private static Connection connectionWithBegin( Consumer<ResponseHandler> beginBehaviour )
    {
        Connection connection = connectionMock();

        doAnswer( invocation ->
                  {
                      ResponseHandler beginHandler = invocation.getArgument( 1 );
                      beginBehaviour.accept( beginHandler );
                      return null;
                  } ).when( connection ).writeAndFlush( argThat( beginMessage() ), any() );

        return connection;
    }
}
