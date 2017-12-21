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
package org.neo4j.driver.internal;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.util.Map;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;

public class NetworkSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Connection connection;
    private ConnectionProvider connectionProvider;
    private NetworkSession session;

    @Before
    public void setUp()
    {
        connection = connectionMock();
        when( connection.release() ).thenReturn( completedWithNull() );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( AccessMode.class ) ) )
                .thenReturn( completedFuture( connection ) );
        session = newSession( connectionProvider, READ );
    }

    @Test
    public void shouldFlushOnRun()
    {
        session.run( "RETURN 1" );

        verify( connection ).runAndFlush( eq( "RETURN 1" ), any(), any(), any() );
    }

    @Test
    public void shouldNotAllowNewTxWhileOneIsRunning()
    {
        // Given
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.beginTransaction();
    }

    @Test
    public void shouldBeAbleToOpenTxAfterPreviousIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        Transaction tx = session.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    public void shouldNotBeAbleToUseSessionWhileOngoingTransaction()
    {
        // Given
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "RETURN 1" );
    }

    @Test
    public void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        session.run( "RETURN 1" );

        // Then
        verify( connection ).runAndFlush( eq( "RETURN 1" ), any(), any(), any() );
    }

    @Test
    public void shouldNotCloseAlreadyClosedSession()
    {
        Transaction tx = session.beginTransaction();

        session.close();
        session.close();
        session.close();

        verifyRollbackTx( connection, times( 1 ) );
    }

    @Test
    public void runThrowsWhenSessionIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );
        NetworkSession session = newSession( connectionProvider, READ );

        session.close();

        try
        {
            session.run( "CREATE ()" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), containsString( "session is already closed" ) );
        }
    }

    @Test
    public void acquiresNewConnectionForRun()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        Connection connection = mock( Connection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( completedFuture( connection ) );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).runAndFlush( eq( "RETURN 1" ), any(), any(), any() );
    }

    @Test
    public void releasesOpenConnectionUsedForRunWhenSessionIsClosed()
    {
        String query = "RETURN 1";
        setupSuccessfulPullAll( query );

        session.run( query );

        await( session.closeAsync() );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).runAndFlush( eq( "RETURN 1" ), any(), any(), any() );
        inOrder.verify( connection, atLeastOnce() ).release();
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void resetDoesNothingWhenNoTransactionAndNoConnection()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        NetworkSession session = newSession( connectionProvider, READ );

        session.reset();

        verify( connectionProvider, never() ).acquireConnection( any( AccessMode.class ) );
    }

    @Test
    public void closeWithoutConnection()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        NetworkSession session = newSession( connectionProvider, READ );

        session.close();

        verify( connectionProvider, never() ).acquireConnection( any( AccessMode.class ) );
    }

    @Test
    public void acquiresNewConnectionForBeginTx()
    {
        Transaction tx = session.beginTransaction();

        assertNotNull( tx );
        verify( connectionProvider ).acquireConnection( READ );
    }

    @Test
    public void updatesBookmarkWhenTxIsClosed()
    {
        Transaction tx = session.beginTransaction();
        setBookmark( tx, Bookmark.from( "TheBookmark" ) );

        assertNull( session.lastBookmark() );

        tx.close();
        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    public void releasesConnectionWhenTxIsClosed()
    {
        String query = "RETURN 42";
        setupSuccessfulPullAll( query );

        Transaction tx = session.beginTransaction();
        tx.run( query );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).runAndFlush( eq( query ), any(), any(), any() );

        tx.close();
        verify( connection ).release();
    }

    @Test
    public void bookmarkCanBeSet()
    {
        Bookmark bookmark = Bookmark.from( "neo4j:bookmark:v1:tx100" );

        session.setBookmark( bookmark );

        assertEquals( bookmark.maxBookmarkAsString(), session.lastBookmark() );
    }

    @Test
    public void bookmarkIsPropagatedFromSession()
    {
        Bookmark bookmark = Bookmark.from( "Bookmark" );
        NetworkSession session = newSession( connectionProvider, READ, bookmark );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );
        verifyBeginTx( connection, bookmark );
    }

    @Test
    public void bookmarkIsPropagatedInBeginTransaction()
    {
        Bookmark bookmark = Bookmark.from( "Bookmark" );
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmark( bookmark );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );
        verifyBeginTx( connection, bookmark );
    }

    @Test
    public void bookmarkIsPropagatedBetweenTransactions()
    {
        Bookmark bookmark1 = Bookmark.from( "Bookmark1" );
        Bookmark bookmark2 = Bookmark.from( "Bookmark2" );

        NetworkSession session = newSession( connectionProvider, READ );

        try ( Transaction tx = session.beginTransaction() )
        {
            setBookmark( tx, bookmark1 );
        }

        assertEquals( bookmark1, Bookmark.from( session.lastBookmark() ) );

        try ( Transaction tx = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmark1 );
            assertTrue( getBookmark( tx ).isEmpty() );
            setBookmark( tx, bookmark2 );
        }
        assertEquals( bookmark2, Bookmark.from( session.lastBookmark() ) );
    }

    @Test
    public void accessModeUsedToAcquireConnections()
    {
        NetworkSession session1 = newSession( connectionProvider, READ );
        session1.beginTransaction();
        verify( connectionProvider ).acquireConnection( READ );

        NetworkSession session2 = newSession( connectionProvider, WRITE );
        session2.beginTransaction();
        verify( connectionProvider ).acquireConnection( WRITE );
    }

    @Test
    public void setLastBookmark()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), WRITE );

        session.setBookmark( Bookmark.from( "TheBookmark" ) );

        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    public void testPassingNoBookmarkShouldRetainBookmark()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmark( Bookmark.from( "X" ) );
        session.beginTransaction();
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void testPassingNullBookmarkShouldRetainBookmark()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmark( Bookmark.from( "X" ) );
        session.beginTransaction( null );
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }

    @Test
    public void acquiresReadConnectionForReadTxInReadSession()
    {
        testConnectionAcquisition( READ, READ );
    }

    @Test
    public void acquiresWriteConnectionForWriteTxInReadSession()
    {
        testConnectionAcquisition( READ, WRITE );
    }

    @Test
    public void acquiresReadConnectionForReadTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, READ );
    }

    @Test
    public void acquiresWriteConnectionForWriteTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, WRITE );
    }

    @Test
    public void commitsReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    public void commitsWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( WRITE, true );
    }

    @Test
    public void rollsBackReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, false );
    }

    @Test
    public void rollsBackWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    public void rollsBackReadTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( READ );
    }

    @Test
    public void rollsBackWriteTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( WRITE );
    }

    @Test
    public void readTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( READ );
    }

    @Test
    public void writeTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( WRITE );
    }

    @Test
    public void readTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( READ );
    }

    @Test
    public void writeTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( WRITE );
    }

    @Test
    public void readTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( READ );
    }

    @Test
    public void writeTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( WRITE );
    }

    @Test
    public void readTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( READ );
    }

    @Test
    public void writeTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( WRITE );
    }

    @Test
    public void connectionShouldBeReleasedAfterSessionReset()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        session.run( "RETURN 1" );

        verify( connection, never() ).release();

        session.reset();
        verify( connection ).release();
    }

    @Test
    public void transactionShouldBeRolledBackAfterSessionReset()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );

        session.reset();
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldHaveNullLastBookmarkInitially()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ );
        assertNull( session.lastBookmark() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithNull()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ, Bookmark.from( "Cat" ) );
        assertEquals( "Cat", session.lastBookmark() );
        session.setBookmark( null );
        assertEquals( "Cat", session.lastBookmark() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        NetworkSession session = newSession( mock( ConnectionProvider.class ), READ, Bookmark.from( "Cat" ) );
        assertEquals( "Cat", session.lastBookmark() );
        session.setBookmark( Bookmark.empty() );
        assertEquals( "Cat", session.lastBookmark() );
    }

    @Test
    public void shouldDoNothingWhenClosingWithoutAcquiredConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( failedFuture( error ) );

        try
        {
            session.run( "RETURN 1" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        session.close();
    }

    @Test
    public void shouldRunAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        try
        {
            session.run( "RETURN 1" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyRunAndFlush( connection, "RETURN 2", times( 1 ) );
    }

    @Test
    public void shouldRunAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock();
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock();

        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmark bookmark = Bookmark.from( "neo4j:bookmark:v1:tx42" );
        session.setBookmark( bookmark );

        try
        {
            session.beginTransaction();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection1, bookmark );
        verifyRunAndFlush( connection2, "RETURN 2", times( 1 ) );
    }

    @Test
    public void shouldBeginTxAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock();
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock();

        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmark bookmark = Bookmark.from( "neo4j:bookmark:v1:tx42" );
        session.setBookmark( bookmark );

        try
        {
            session.beginTransaction();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection1, bookmark );
        verifyBeginTx( connection2, bookmark );
    }

    @Test
    public void shouldBeginTxAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( READ ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        try
        {
            session.run( "RETURN 1" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verifyBeginTx( connection, times( 1 ) );
    }

    @Test
    public void shouldMarkTransactionAsTerminatedAndThenReleaseConnectionOnReset()
    {
        NetworkSession session = newSession( connectionProvider, READ );
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );
        when( connection.release() ).then( invocation ->
        {
            // verify that tx is not open when connection is released
            assertFalse( tx.isOpen() );
            return completedWithNull();
        } );

        session.reset();

        verify( connection ).release();
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

        try
        {
            executeTransaction( session, transactionMode, work );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

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
        try
        {
            executeTransaction( session, mode, work );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertEquals( "Oh!", e.getMessage() );
            verifyInvocationCount( work, failures );
            verifyCommitTx( connection, never() );
            verifyRollbackTx( connection, times( failures ) );
        }
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails( AccessMode mode )
    {
        int failures = 17;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        setupFailingCommit( connection, failures );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 42 ) );
        try
        {
            executeTransaction( session, mode, work );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            verifyInvocationCount( work, failures );
            verifyCommitTx( connection, times( failures ) );
        }
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
        return newSession( connectionProvider, mode, Bookmark.empty() );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, RetryLogic retryLogic )
    {
        return newSession( connectionProvider, WRITE, retryLogic, Bookmark.empty() );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode,
            Bookmark bookmark )
    {
        return newSession( connectionProvider, mode, new FixedRetryLogic( 0 ), bookmark );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode,
            RetryLogic retryLogic, Bookmark bookmark )
    {
        NetworkSession session = new NetworkSession( connectionProvider, mode, retryLogic, DEV_NULL_LOGGING );
        session.setBookmark( bookmark );
        return session;
    }

    private static void verifyInvocationCount( TransactionWork<?> workSpy, int expectedInvocationCount )
    {
        verify( workSpy, times( expectedInvocationCount ) ).execute( any( Transaction.class ) );
    }

    private static void verifyBeginTx( Connection connectionMock, VerificationMode mode )
    {
        verify( connectionMock, mode ).run( eq( "BEGIN" ), any(), any(), any() );
    }

    private static void verifyBeginTx( Connection connectionMock, Bookmark bookmark )
    {
        if ( bookmark.isEmpty() )
        {
            verify( connectionMock ).run( eq( "BEGIN" ), any(), any(), any() );
        }
        else
        {
            Map<String,Value> params = bookmark.asBeginTransactionParameters();
            verify( connectionMock ).runAndFlush( eq( "BEGIN" ), eq( params ), any(), any() );
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
        verify( connectionMock, mode ).runAndFlush( eq( statement ), any(), any(), any() );
    }

    private static Bookmark getBookmark( Transaction tx )
    {
        return ((ExplicitTransaction) tx).bookmark();
    }

    private static void setBookmark( Transaction tx, Bookmark bookmark )
    {
        ((ExplicitTransaction) tx).setBookmark( bookmark );
    }

    private static void setupFailingCommit( Connection connection, int times )
    {
        doAnswer( new Answer<Void>()
        {
            int invoked;

            @Override
            public Void answer( InvocationOnMock invocation )
            {
                ResponseHandler handler = invocation.getArgumentAt( 3, ResponseHandler.class );
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
        } ).when( connection ).runAndFlush( eq( "COMMIT" ), any(), any(), any() );
    }

    private static void setupFailingBegin( Connection connection, Throwable error )
    {
        doAnswer( (Answer<Void>) invocation ->
        {
            ResponseHandler handler = invocation.getArgumentAt( 3, ResponseHandler.class );
            handler.onFailure( error );
            return null;
        } ).when( connection ).runAndFlush( eq( "BEGIN" ), any(), any(), any() );
    }

    private void setupSuccessfulPullAll( String query )
    {
        doAnswer( invocation ->
        {
            ResponseHandler pullAllHandler = invocation.getArgumentAt( 3, ResponseHandler.class );
            pullAllHandler.onSuccess( emptyMap() );
            return null;
        } ).when( connection ).runAndFlush( eq( query ), eq( emptyMap() ), any(), any() );
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
