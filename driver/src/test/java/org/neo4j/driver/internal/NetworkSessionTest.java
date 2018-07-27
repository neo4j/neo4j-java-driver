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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.util.Map;

import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.retry.FixedRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;
import static org.neo4j.driver.v1.util.TestUtil.DEFAULT_TEST_PROTOCOL;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;
import static org.neo4j.driver.v1.util.TestUtil.runMessageWithStatementMatcher;

class NetworkSessionTest
{
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private NetworkSession session;

    @BeforeEach
    void setUp()
    {
        connection = connectionMock();
        when( connection.release() ).thenReturn( completedWithNull() );
        when( connection.reset() ).thenReturn( completedWithNull() );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        when( connection.protocol() ).thenReturn( DEFAULT_TEST_PROTOCOL );
        connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( AccessMode.class ) ) )
                .thenReturn( completedFuture( connection ) );
        session = newSession( connectionProvider, READ );
    }

    @Test
    void shouldFlushOnRun()
    {
        session.run( "RETURN 1" );

        verify( connection ).writeAndFlush( eq( new RunMessage( "RETURN 1" ) ), any(), any(), any() );
    }

    @Test
    void shouldNotAllowNewTxWhileOneIsRunning()
    {
        // Given
        session.beginTransaction();

        // Expect
        assertThrows( ClientException.class, session::beginTransaction );
    }

    @Test
    void shouldBeAbleToOpenTxAfterPreviousIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        Transaction tx = session.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    void shouldNotBeAbleToUseSessionWhileOngoingTransaction()
    {
        // Given
        session.beginTransaction();

        // Expect
        assertThrows( ClientException.class, () -> session.run( "RETURN 1" ) );
    }

    @Test
    void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        session.run( "RETURN 1" );

        // Then
        verify( connection ).writeAndFlush( eq( new RunMessage( "RETURN 1" ) ), any(), any(), any() );
    }

    @Test
    void shouldNotCloseAlreadyClosedSession()
    {
        Transaction tx = session.beginTransaction();

        session.close();
        session.close();
        session.close();

        verifyRollbackTx( connection, times( 1 ) );
    }

    @Test
    void runThrowsWhenSessionIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );
        NetworkSession session = newSession( connectionProvider, READ );

        session.close();

        Exception e = assertThrows( Exception.class, () -> session.run( "CREATE ()" ) );
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( e.getMessage(), containsString( "session is already closed" ) );
    }

    @Test
    void acquiresNewConnectionForRun()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        Connection connection = connectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( completedFuture( connection ) );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).writeAndFlush( eq( new RunMessage( "RETURN 1" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
    }

    @Test
    void releasesOpenConnectionUsedForRunWhenSessionIsClosed()
    {
        String query = "RETURN 1";
        setupSuccessfulPullAll( query );

        session.run( query );

        await( session.closeAsync() );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).writeAndFlush( eq( new RunMessage( "RETURN 1" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        inOrder.verify( connection, atLeastOnce() ).release();
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void resetDoesNothingWhenNoTransactionAndNoConnection()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        NetworkSession session = newSession( connectionProvider, READ );

        session.reset();

        verify( connectionProvider, never() ).acquireConnection( any( AccessMode.class ) );
    }

    @Test
    void closeWithoutConnection()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        NetworkSession session = newSession( connectionProvider, READ );

        session.close();

        verify( connectionProvider, never() ).acquireConnection( any( AccessMode.class ) );
    }

    @Test
    void acquiresNewConnectionForBeginTx()
    {
        Transaction tx = session.beginTransaction();

        assertNotNull( tx );
        verify( connectionProvider ).acquireConnection( READ );
    }

    @Test
    void updatesBookmarkWhenTxIsClosed()
    {
        Transaction tx = session.beginTransaction();
        setBookmarks( tx, Bookmarks.from( "TheBookmark" ) );

        assertNull( session.lastBookmark() );

        tx.close();
        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    void releasesConnectionWhenTxIsClosed()
    {
        String query = "RETURN 42";
        setupSuccessfulPullAll( query );

        Transaction tx = session.beginTransaction();
        tx.run( query );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).writeAndFlush( eq( new RunMessage( query ) ), any(), any(), any() );

        tx.close();
        verify( connection ).release();
    }

    @Test
    void bookmarkCanBeSet()
    {
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx100" );

        session.setBookmarks( bookmarks );

        assertEquals( bookmarks.maxBookmarkAsString(), session.lastBookmark() );
    }

    @Test
    void bookmarkIsPropagatedFromSession()
    {
        Bookmarks bookmarks = Bookmarks.from( "Bookmarks" );
        NetworkSession session = newSession( connectionProvider, READ, bookmarks );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );
        verifyBeginTx( connection, bookmarks );
    }

    @Test
    void bookmarkIsPropagatedInBeginTransaction()
    {
        Bookmarks bookmarks = Bookmarks.from( "Bookmarks" );
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmarks( bookmarks );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );
        verifyBeginTx( connection, bookmarks );
    }

    @Test
    void bookmarkIsPropagatedBetweenTransactions()
    {
        Bookmarks bookmarks1 = Bookmarks.from( "Bookmark1" );
        Bookmarks bookmarks2 = Bookmarks.from( "Bookmark2" );

        NetworkSession session = newSession( connectionProvider, READ );

        try ( Transaction tx = session.beginTransaction() )
        {
            setBookmarks( tx, bookmarks1 );
        }

        assertEquals( bookmarks1, Bookmarks.from( session.lastBookmark() ) );

        try ( Transaction tx = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmarks1 );
            assertTrue( getBookmarks( tx ).isEmpty() );
            setBookmarks( tx, bookmarks2 );
        }
        assertEquals( bookmarks2, Bookmarks.from( session.lastBookmark() ) );
    }

    @Test
    void accessModeUsedToAcquireConnections()
    {
        NetworkSession session1 = newSession( connectionProvider, READ );
        session1.beginTransaction();
        verify( connectionProvider ).acquireConnection( READ );

        NetworkSession session2 = newSession( connectionProvider, WRITE );
        session2.beginTransaction();
        verify( connectionProvider ).acquireConnection( WRITE );
    }

    @Test
    void setLastBookmark()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), WRITE );

        session.setBookmarks( Bookmarks.from( "TheBookmark" ) );

        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    void testPassingNoBookmarkShouldRetainBookmark()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmarks( Bookmarks.from( "X" ) );
        session.beginTransaction();
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void testPassingNullBookmarkShouldRetainBookmark()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmarks( Bookmarks.from( "X" ) );
        session.beginTransaction( (String) null );
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }

    @Test
    void acquiresReadConnectionForReadTxInReadSession()
    {
        testConnectionAcquisition( READ, READ );
    }

    @Test
    void acquiresWriteConnectionForWriteTxInReadSession()
    {
        testConnectionAcquisition( READ, WRITE );
    }

    @Test
    void acquiresReadConnectionForReadTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, READ );
    }

    @Test
    void acquiresWriteConnectionForWriteTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, WRITE );
    }

    @Test
    void commitsReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    void commitsWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( WRITE, true );
    }

    @Test
    void rollsBackReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, false );
    }

    @Test
    void rollsBackWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    void rollsBackReadTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( READ );
    }

    @Test
    void rollsBackWriteTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( WRITE );
    }

    @Test
    void connectionShouldBeResetAfterSessionReset()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.run( "RETURN 1" );

        verify( connection, never() ).reset();
        verify( connection, never() ).release();

        session.reset();
        verify( connection ).reset();
        verify( connection, never() ).release();
    }

    @Test
    void shouldHaveNullLastBookmarkInitially()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ );
        assertNull( session.lastBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithNull()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ, Bookmarks.from( "Cat" ) );
        assertEquals( "Cat", session.lastBookmark() );
        session.setBookmarks( null );
        assertEquals( "Cat", session.lastBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ, Bookmarks.from( "Cat" ) );
        assertEquals( "Cat", session.lastBookmark() );
        session.setBookmarks( Bookmarks.empty() );
        assertEquals( "Cat", session.lastBookmark() );
    }

    @Test
    void shouldDoNothingWhenClosingWithoutAcquiredConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.close();
    }

    @Test
    void shouldRunAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyRunAndFlush( connection, "RETURN 2", times( 1 ) );
    }

    @Test
    void shouldRunAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock();
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock();

        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx42" );
        session.setBookmarks( bookmarks );

        Exception e = assertThrows( Exception.class, session::beginTransaction );
        assertEquals( error, e );

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection1, bookmarks );
        verifyRunAndFlush( connection2, "RETURN 2", times( 1 ) );
    }

    @Test
    void shouldBeginTxAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock();
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock();

        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx42" );
        session.setBookmarks( bookmarks );

        Exception e = assertThrows( Exception.class, session::beginTransaction );
        assertEquals( error, e );

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection1, bookmarks );
        verifyBeginTx( connection2, bookmarks );
    }

    @Test
    void shouldBeginTxAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection, times( 1 ) );
    }

    @Test
    void shouldMarkTransactionAsTerminatedAndThenResetConnectionOnReset()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );
        verify( connection, never() ).reset();

        session.reset();

        verify( connection ).reset();
    }

    @Test
    void shouldNotAllowStartingMultipleTransactions()
    {
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );

        for ( int i = 0; i < 5; i++ )
        {
            ClientException e = assertThrows( ClientException.class, session::beginTransaction );
            assertThat( e.getMessage(),
                    containsString( "You cannot begin a transaction on a session with an open transaction" ) );
        }
    }

    @Test
    void shouldAllowStartingTransactionAfterCurrentOneIsClosed()
    {
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );

        ClientException e = assertThrows( ClientException.class, session::beginTransaction );
        assertThat( e.getMessage(),
                containsString( "You cannot begin a transaction on a session with an open transaction" ) );

        tx.close();

        assertNotNull( session.beginTransaction() );
    }

    private void testConnectionAcquisition( AccessMode sessionMode, AccessMode transactionMode )
    {
        NetworkSession session = newSession( connectionProvider, sessionMode );

        TxWork work = new TxWork( 42 );

        int result = executeTransaction( session, transactionMode, work );

        verify( connectionProvider ).acquireConnection( transactionMode );
        verifyBeginTx( connection, times( 1 ) );
        verifyCommitTx( connection, times( 1 ) );
        assertEquals( 42, result );
    }

    private void testTxCommitOrRollback( AccessMode transactionMode, final boolean commit )
    {
        NetworkSession session = newSession( connectionProvider, WRITE );

        TransactionWork<Integer> work = tx ->
        {
            if ( commit )
            {
                tx.success();
            }
            else
            {
                tx.failure();
            }
            return 4242;
        };

        int result = executeTransaction( session, transactionMode, work );

        verify( connectionProvider ).acquireConnection( transactionMode );
        verifyBeginTx( connection, times( 1 ) );
        if ( commit )
        {
            verifyCommitTx( connection, times( 1 ) );
            verifyRollbackTx( connection, never() );
        }
        else
        {
            verifyRollbackTx( connection, times( 1 ) );
            verifyCommitTx( connection, never() );
        }
        assertEquals( 4242, result );
    }

    private void testTxRollbackWhenThrows( AccessMode transactionMode )
    {
        NetworkSession session = newSession( connectionProvider, WRITE );

        final RuntimeException error = new IllegalStateException( "Oh!" );
        TransactionWork<Void> work = tx ->
        {
            throw error;
        };

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, transactionMode, work ) );
        assertEquals( error, e );

        verify( connectionProvider ).acquireConnection( transactionMode );
        verifyBeginTx( connection, times( 1 ) );
        verifyRollbackTx( connection, times( 1 ) );
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows( AccessMode mode )
    {
        int failures = 12;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "" ) ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 42, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( 1 ) );
        verifyRollbackTx( connection, times( failures ) );
    }

    private void testTxIsRetriedUntilSuccessWhenCommitThrows( AccessMode mode )
    {
        int failures = 13;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        setupFailingCommit( connection, failures );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 43 ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 43, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( retries ) );
    }

    private void testTxIsRetriedUntilFailureWhenFunctionThrows( AccessMode mode )
    {
        int failures = 14;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "Oh!" ) ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( SessionExpiredException.class ) );
        assertEquals( "Oh!", e.getMessage() );
        verifyInvocationCount( work, failures );
        verifyCommitTx( connection, never() );
        verifyRollbackTx( connection, times( failures ) );
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails( AccessMode mode )
    {
        int failures = 17;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        setupFailingCommit( connection, failures );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 42 ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        verifyInvocationCount( work, failures );
        verifyCommitTx( connection, times( failures ) );
    }

    private static <T> T executeTransaction( Session session, AccessMode mode, TransactionWork<T> work )
    {
        if ( mode == READ )
        {
            return session.readTransaction( work );
        }
        else if ( mode == WRITE )
        {
            return session.writeTransaction( work );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown mode " + mode );
        }
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode )
    {
        return newSession( connectionProvider, mode, Bookmarks.empty() );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, RetryLogic retryLogic )
    {
        return newSession( connectionProvider, WRITE, retryLogic, Bookmarks.empty() );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode,
            Bookmarks bookmarks )
    {
        return newSession( connectionProvider, mode, new FixedRetryLogic( 0 ), bookmarks );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode,
            RetryLogic retryLogic, Bookmarks bookmarks )
    {
        NetworkSession session = new NetworkSession( connectionProvider, mode, retryLogic, DEV_NULL_LOGGING );
        session.setBookmarks( bookmarks );
        return session;
    }

    private static void verifyInvocationCount( TransactionWork<?> workSpy, int expectedInvocationCount )
    {
        verify( workSpy, times( expectedInvocationCount ) ).execute( any( Transaction.class ) );
    }

    private static void verifyBeginTx( Connection connectionMock, VerificationMode mode )
    {
        verify( connectionMock, mode ).write( eq( new RunMessage( "BEGIN" ) ), any(), any(), any() );
    }

    private static void verifyBeginTx( Connection connectionMock, Bookmarks bookmarks )
    {
        if ( bookmarks.isEmpty() )
        {
            verify( connectionMock ).write( eq( new RunMessage( "BEGIN" ) ), any(), any(), any() );
        }
        else
        {
            Map<String,Value> params = bookmarks.asBeginTransactionParameters();
            verify( connectionMock ).writeAndFlush( eq( new RunMessage( "BEGIN", params ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
        }
    }

    private static void verifyCommitTx( Connection connectionMock, VerificationMode mode )
    {
        verifyRunAndFlush( connectionMock, "COMMIT", mode );
    }

    private static void verifyRollbackTx( Connection connectionMock, VerificationMode mode )
    {
        verifyRunAndFlush( connectionMock, "ROLLBACK", mode );
    }

    private static void verifyRunAndFlush( Connection connectionMock, String statement, VerificationMode mode )
    {
        verify( connectionMock, mode ).writeAndFlush( eq( new RunMessage( statement ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
    }

    private static Bookmarks getBookmarks( Transaction tx )
    {
        return ((ExplicitTransaction) tx).bookmark();
    }

    private static void setBookmarks( Transaction tx, Bookmarks bookmarks )
    {
        ((ExplicitTransaction) tx).setBookmarks( bookmarks );
    }

    private static void setupFailingCommit( Connection connection, int times )
    {
        doAnswer( new Answer<Void>()
        {
            int invoked;

            @Override
            public Void answer( InvocationOnMock invocation )
            {
                ResponseHandler handler = invocation.getArgument( 3 );
                if ( invoked++ < times )
                {
                    handler.onFailure( new ServiceUnavailableException( "" ) );
                }
                else
                {
                    handler.onSuccess( emptyMap() );
                }
                return null;
            }
        } ).when( connection ).writeAndFlush( eq( new RunMessage( "COMMIT" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
    }

    private static void setupFailingBegin( Connection connection, Throwable error )
    {
        doAnswer( invocation ->
        {
            ResponseHandler handler = invocation.getArgument( 3 );
            handler.onFailure( error );
            return null;
        } ).when( connection ).writeAndFlush( argThat( runMessageWithStatementMatcher( "BEGIN" ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
    }

    private void setupSuccessfulPullAll( String query )
    {
        doAnswer( invocation ->
        {
            ResponseHandler pullAllHandler = invocation.getArgument( 3 );
            pullAllHandler.onSuccess( emptyMap() );
            return null;
        } ).when( connection ).writeAndFlush( eq( new RunMessage( query ) ), any(), eq( PullAllMessage.PULL_ALL ), any() );
    }

    private static class TxWork implements TransactionWork<Integer>
    {
        final int result;
        final int timesToThrow;
        final Supplier<RuntimeException> errorSupplier;

        int invoked;

        @SuppressWarnings( "unchecked" )
        TxWork( int result )
        {
            this( result, 0, (Supplier) null );
        }

        TxWork( int result, int timesToThrow, final RuntimeException error )
        {
            this.result = result;
            this.timesToThrow = timesToThrow;
            this.errorSupplier = () -> error;
        }

        TxWork( int result, int timesToThrow, Supplier<RuntimeException> errorSupplier )
        {
            this.result = result;
            this.timesToThrow = timesToThrow;
            this.errorSupplier = errorSupplier;
        }

        @Override
        public Integer execute( Transaction tx )
        {
            if ( timesToThrow > 0 && invoked++ < timesToThrow )
            {
                throw errorSupplier.get();
            }
            tx.success();
            return result;
        }
    }
}
