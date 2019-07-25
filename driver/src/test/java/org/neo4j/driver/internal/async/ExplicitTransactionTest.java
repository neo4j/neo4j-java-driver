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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.util.function.Consumer;

import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.DefaultBookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.runMessageWithStatementMatcher;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRun;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyRun;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;

class ExplicitTransactionTest
{
    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldFlushOnRunAsync( boolean waitForResponse )
    {
        // Given
        Connection connection = connectionMock( BoltProtocolV4.INSTANCE );
        ExplicitTransaction tx = beginTx( connection );
        setupSuccessfulRunAndPull( connection );

        // When
        await( tx.runAsync( new Statement( "RETURN 1" ), waitForResponse ) );

        // Then
        verifyRunAndPull( connection, "RETURN 1" );
    }

    @Test
    void shouldFlushOnRunRx()
    {
        // Given
        Connection connection = connectionMock( BoltProtocolV4.INSTANCE );
        ExplicitTransaction tx = beginTx( connection );
        setupSuccessfulRun( connection );

        // When
        await( tx.runRx( new Statement( "RETURN 1" ) ) );

        // Then
        verifyRun( connection, "RETURN 1" );
    }

    @Test
    void shouldRollbackOnImplicitFailure()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        await( tx.closeAsync() );

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).write( eq( new RunMessage( "BEGIN" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).writeAndFlush( eq( new RunMessage( "ROLLBACK" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).release();
    }

    @Test
    void shouldRollbackOnExplicitFailure()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        tx.failure();
        tx.success(); // even if success is called after the failure call!
        await( tx.closeAsync() );

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).write( eq( new RunMessage( "BEGIN" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).writeAndFlush( eq( new RunMessage( "ROLLBACK" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).release();
    }

    @Test
    void shouldCommitOnSuccess()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        tx.success();
        await( tx.closeAsync() );

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).write( eq( new RunMessage( "BEGIN" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).writeAndFlush( eq( new RunMessage( "COMMIT" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        order.verify( connection ).release();
    }

    @Test
    void shouldOnlyQueueMessagesWhenNoBookmarkGiven()
    {
        Connection connection = connectionMock();

        beginTx( connection, InternalBookmark.empty() );

        verify( connection ).write( eq( new RunMessage( "BEGIN" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        verify( connection, never() ).writeAndFlush( any(), any(), any(), any() );
    }

    @Test
    void shouldFlushWhenBookmarkGiven()
    {
        InternalBookmark bookmark = InternalBookmark.parse( "hi, I'm bookmark" );
        Connection connection = connectionMock();

        beginTx( connection, bookmark );

        verify( connection ).writeAndFlush( any(), any(), eq( PullAllMessage.PULL_ALL ), any() );
        verify( connection, never() ).write( any(), any(), any(), any() );
    }

    @Test
    void shouldBeOpenAfterConstruction()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForSuccess()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.success();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForFailure()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.failure();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedAsTerminated()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.markTerminated();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterCommit()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.success();
        await( tx.closeAsync() );

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterRollback()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.failure();
        await( tx.closeAsync() );

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedTerminatedAndClosed()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.markTerminated();
        await( tx.closeAsync() );

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenBeginFails()
    {
        RuntimeException error = new RuntimeException( "Wrong bookmark!" );
        Connection connection = connectionWithBegin( handler -> handler.onFailure( error ) );
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );

        InternalBookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        TransactionConfig txConfig = TransactionConfig.empty();

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( tx.beginAsync( bookmark, txConfig ) ) );

        assertEquals( error, e );
        verify( connection ).release();
    }

    @Test
    void shouldNotReleaseConnectionWhenBeginSucceeds()
    {
        Connection connection = connectionWithBegin( handler -> handler.onSuccess( emptyMap() ) );
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );

        InternalBookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        TransactionConfig txConfig = TransactionConfig.empty();

        await( tx.beginAsync( bookmark, txConfig ) );

        verify( connection, never() ).release();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndCommitted()
    {
        Connection connection = connectionMock();
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );

        tx.markTerminated();

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );

        assertFalse( tx.isOpen() );
        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndRolledBack()
    {
        Connection connection = connectionMock();
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );

        tx.markTerminated();
        await( tx.rollbackAsync() );

        verify( connection ).release();
    }

    @Test
    void shouldReleaseConnectionWhenClose() throws Throwable
    {
        Connection connection = connectionMock();
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );

        await( tx.closeAsync() );

        verify( connection ).release();
    }

    private static ExplicitTransaction beginTx( Connection connection )
    {
        return beginTx( connection, InternalBookmark.empty() );
    }

    private static ExplicitTransaction beginTx( Connection connection, InternalBookmark initialBookmark )
    {
        ExplicitTransaction tx = new ExplicitTransaction( connection, new DefaultBookmarkHolder() );
        return await( tx.beginAsync( initialBookmark, TransactionConfig.empty() ) );
    }

    private static Connection connectionWithBegin( Consumer<ResponseHandler> beginBehaviour )
    {
        Connection connection = connectionMock();

        doAnswer( invocation ->
        {
            ResponseHandler beginHandler = invocation.getArgument( 3 );
            beginBehaviour.accept( beginHandler );
            return null;
        } ).when( connection ).writeAndFlush( argThat( runMessageWithStatementMatcher( "BEGIN" ) ), any(), any(), any() );

        return connection;
    }
}
