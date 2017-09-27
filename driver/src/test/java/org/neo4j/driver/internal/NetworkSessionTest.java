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

import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.retry.FixedRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

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
import static org.mockito.Mockito.anyMapOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

public class NetworkSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PooledConnection connection;
    private NetworkSession session;

    @Before
    public void setUp() throws Exception
    {
        connection = mock( PooledConnection.class );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( AccessMode.class ) ) ).thenReturn( connection );
        session = newSession( connectionProvider, READ );
    }

    @Test
    public void shouldSendAllOnRun() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( true );

        // When
        session.run( "whatever" );

        // Then
        verify( connection ).flush();
    }

    @Test
    public void shouldNotAllowNewTxWhileOneIsRunning() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( true );
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.beginTransaction();
    }

    @Test
    public void shouldBeAbleToOpenTxAfterPreviousIsClosed() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( true );
        session.beginTransaction().close();

        // When
        Transaction tx = session.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    public void shouldNotBeAbleToUseSessionWhileOngoingTransaction() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( true );
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "whatever" );
    }

    @Test
    public void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( true );
        session.beginTransaction().close();

        // When
        session.run( "whatever" );

        // Then
        verify( connection ).flush();
    }

    @Test
    public void shouldGetExceptionIfTryingToCloseSessionMoreThanOnce() throws Throwable
    {
        // Given
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );
        NetworkSession sess = newSession( connectionProvider, READ );
        try
        {
            sess.close();
        }
        catch ( Exception e )
        {
            fail( "Should not get any problem to close first time" );
        }

        // When
        try
        {
            sess.close();
            fail( "Should have received an error to close second time" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), equalTo( "This session has already been closed." ) );
        }
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
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );
    }

    @Test
    public void syncsAndClosesPreviousConnectionForRun()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = openConnectionMock();
        PooledConnection connection2 = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.run( "RETURN 2" );
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verify( connection2 ).run( eq( "RETURN 2" ), anyParams(), any( ResponseHandler.class ) );

        InOrder inOrder = inOrder( connection1 );
        inOrder.verify( connection1 ).sync();
        inOrder.verify( connection1 ).close();
    }

    @Test
    public void closesPreviousBrokenConnectionForRun()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.run( "RETURN 2" );
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verify( connection2 ).run( eq( "RETURN 2" ), anyParams(), any( ResponseHandler.class ) );

        verify( connection1, never() ).sync();
        verify( connection1 ).close();
    }

    @Test
    public void closesAndSyncOpenConnectionUsedForRunWhenSessionIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.close();

        verify( connection ).sync();
        verify( connection ).close();
    }

    @Test
    public void closesClosedConnectionUsedForRunWhenSessionIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.close();

        verify( connection, never() ).sync();
        verify( connection ).close();
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
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.beginTransaction();
        verify( connectionProvider ).acquireConnection( READ );
    }

    @Test
    public void closesPreviousConnectionForBeginTx()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.beginTransaction();
        verify( connection1 ).close();
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
    }

    @Test
    public void updatesBookmarkWhenTxIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        setBookmark( tx, Bookmark.from( "TheBookmark" ) );

        assertNull( session.lastBookmark() );

        tx.close();
        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    public void closesConnectionWhenTxIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        tx.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        tx.close();
        verify( connection ).sync();
        verify( connection ).close();
    }

    @Test
    public void ignoresWronglyClosedTx()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = openConnectionMock();
        PooledConnection connection2 = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx1 = session.beginTransaction();
        tx1.close();

        Transaction tx2 = session.beginTransaction();
        tx2.close();

        tx1.close();

        verify( connection1 ).sync();
        verify( connection1 ).close();

        verify( connection2 ).sync();
        verify( connection2 ).close();
    }

    @Test
    public void ignoresWronglyClosedTxWhenAnotherTxInProgress()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = openConnectionMock();
        PooledConnection connection2 = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx1 = session.beginTransaction();
        tx1.close();

        Transaction tx2 = session.beginTransaction();
        tx1.close();
        tx2.close();

        verify( connection1 ).sync();
        verify( connection1 ).close();

        verify( connection2 ).sync();
        verify( connection2 ).close();
    }

    @Test
    public void transactionClosedDoesNothingWhenNoTx()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.onTransactionClosed( mock( ExplicitTransaction.class ) );

        verifyZeroInteractions( connection );
    }

    @Test
    public void transactionClosedIgnoresWrongTx()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.beginTransaction();
        verify( connectionProvider ).acquireConnection( READ );

        ExplicitTransaction wrongTx = mock( ExplicitTransaction.class );
        session.onTransactionClosed( wrongTx );

        verify( connection, never() ).close();
    }

    @Test
    public void markTxAsFailedOnRecoverableConnectionError()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertTrue( tx.isOpen() );

        session.onConnectionError( true );

        assertTrue( tx.isOpen() );
    }

    @Test
    public void markTxToCloseOnUnrecoverableConnectionError()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertTrue( tx.isOpen() );

        session.onConnectionError( false );

        assertTrue( tx.isOpen() );
    }

    @Test
    public void closesConnectionWhenResultIsBuffered()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( ResponseHandler.class ) );

        session.onResultConsumed();

        verify( connection, never() ).sync();
        verify( connection ).close();
    }

    @Test
    public void bookmarkIsPropagatedFromSession()
    {
        Bookmark bookmark = Bookmark.from( "Bookmark" );

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ, bookmark );

        try ( Transaction ignore = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmark );
        }
    }

    @Test
    public void bookmarkIsPropagatedInBeginTransaction()
    {
        Bookmark bookmark = Bookmark.from( "Bookmark" );

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmark(bookmark);

        try ( Transaction ignore = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmark );
        }
    }

    @Test
    public void bookmarkIsPropagatedBetweenTransactions()
    {
        Bookmark bookmark1 = Bookmark.from( "Bookmark1" );
        Bookmark bookmark2 = Bookmark.from( "Bookmark2" );

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        try ( Transaction tx = session.beginTransaction() )
        {
            setBookmark( tx, bookmark1 );
        }

        assertEquals( bookmark1.maxBookmarkAsString(), session.lastBookmark() );

        try ( Transaction tx = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmark1 );
            assertTrue( getBookmark( tx ).isEmpty() );
            setBookmark( tx, bookmark2 );
        }

        assertEquals( bookmark2.maxBookmarkAsString(), session.lastBookmark() );
    }

    @Test
    public void accessModeUsedToAcquireConnections()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );

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
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );
        session.setBookmark( Bookmark.from( "X" ) );
        session.beginTransaction();
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void testPassingNullBookmarkShouldRetainBookmark()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
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
        testTxIsRetriedUntilSuccessWhenTxCloseThrows( READ );
    }

    @Test
    public void writeTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenTxCloseThrows( WRITE );
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
        testTxIsRetriedUntilFailureWhenTxCloseThrows( READ );
    }

    @Test
    public void writeTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenTxCloseThrows( WRITE );
    }

    @Test
    @SuppressWarnings( "deprecation" )
    public void transactionShouldBeOpenAfterSessionReset()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );

        session.reset();
        assertTrue( tx.isOpen() );
    }

    @Test
    @SuppressWarnings( "deprecation" )
    public void transactionShouldBeClosedAfterSessionResetAndClose()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );

        session.reset();
        assertTrue( tx.isOpen() );

        tx.close();
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

    private static void testConnectionAcquisition( AccessMode sessionMode, AccessMode transactionMode )
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( transactionMode ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, sessionMode );

        TxWork work = new TxWork( 42 );

        int result = executeTransaction( session, transactionMode, work );

        verify( connectionProvider ).acquireConnection( transactionMode );
        verifyBeginTx( connection, times( 1 ) );
        verifyCommitTx( connection, times( 1 ) );
        assertEquals( 42, result );
    }

    private static void testTxCommitOrRollback( AccessMode transactionMode, final boolean commit )
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( transactionMode ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, WRITE );

        TransactionWork<Integer> work = new TransactionWork<Integer>()
        {
            @Override
            public Integer execute( Transaction tx )
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
            }
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

    private static void testTxRollbackWhenThrows( AccessMode transactionMode )
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( transactionMode ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, WRITE );

        final RuntimeException error = new IllegalStateException( "Oh!" );
        TransactionWork<Void> work = new TransactionWork<Void>()
        {
            @Override
            public Void execute( Transaction tx )
            {
                throw error;
            }
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

    private static void testTxIsRetriedUntilSuccessWhenFunctionThrows( AccessMode mode )
    {
        int failures = 12;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( mode ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "" ) ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 42, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( 1 ) );
        verifyRollbackTx( connection, times( failures ) );
    }

    private static void testTxIsRetriedUntilSuccessWhenTxCloseThrows( AccessMode mode )
    {
        int failures = 13;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = connectionWithFailingCommit( failures );
        when( connectionProvider.acquireConnection( mode ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, retryLogic );

        TxWork work = spy( new TxWork( 43 ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 43, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( retries ) );
        verifyRollbackTx( connection, times( failures ) );
    }

    private static void testTxIsRetriedUntilFailureWhenFunctionThrows( AccessMode mode )
    {
        int failures = 14;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = openConnectionMock();
        when( connectionProvider.acquireConnection( mode ) ).thenReturn( connection );
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

    private static void testTxIsRetriedUntilFailureWhenTxCloseThrows( AccessMode mode )
    {
        int failures = 17;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = connectionWithFailingCommit( failures );
        when( connectionProvider.acquireConnection( mode ) ).thenReturn( connection );
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
            verifyRollbackTx( connection, times( failures ) );
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

    private static PooledConnection openConnectionMock()
    {
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        return connection;
    }

    private static PooledConnection connectionWithFailingCommit( final int times )
    {
        PooledConnection connection = openConnectionMock();

        doAnswer( new Answer<Void>()
        {
            int invoked;

            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                if ( invoked++ < times )
                {
                    throw new ServiceUnavailableException( "" );
                }
                return null;
            }
        } ).when( connection ).run( eq( "COMMIT" ), anyParams(), any( ResponseHandler.class ) );

        return connection;
    }

    private static void verifyInvocationCount( TransactionWork<?> workSpy, int expectedInvocationCount )
    {
        verify( workSpy, times( expectedInvocationCount ) ).execute( any( Transaction.class ) );
    }

    private static void verifyBeginTx( PooledConnection connectionMock, VerificationMode mode )
    {
        verifyRun( connectionMock, "BEGIN", mode );
    }

    private static void verifyBeginTx( PooledConnection connectionMock, Bookmark bookmark )
    {
        verify( connectionMock ).run( "BEGIN", bookmark.asBeginTransactionParameters(), NoOpResponseHandler.INSTANCE );
    }

    private static void verifyCommitTx( PooledConnection connectionMock, VerificationMode mode )
    {
        verifyRun( connectionMock, "COMMIT", mode );
    }

    private static void verifyRollbackTx( PooledConnection connectionMock, VerificationMode mode )
    {
        verifyRun( connectionMock, "ROLLBACK", mode );
    }

    private static void verifyRun( PooledConnection connectionMock, String statement, VerificationMode mode )
    {
        verify( connectionMock, mode ).run( eq( statement ), anyParams(), any( ResponseHandler.class ) );
    }

    private static Map<String,Value> anyParams()
    {
        return anyMapOf( String.class, Value.class );
    }

    private static Bookmark getBookmark( Transaction tx )
    {
        return ((ExplicitTransaction) tx).bookmark();
    }

    private static void setBookmark( Transaction tx, Bookmark bookmark )
    {
        ((ExplicitTransaction) tx).setBookmark( bookmark );
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
            this.errorSupplier = new Supplier<RuntimeException>()
            {
                @Override
                public RuntimeException get()
                {
                    return error;
                }
            };
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
