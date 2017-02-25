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

import java.util.Map;

import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.singletonMap;
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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.spi.Collector.NO_OP;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;
import static org.neo4j.driver.v1.Values.value;

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
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );
    }

    @Test
    public void syncsAndClosesPreviousConnectionForRun()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = mock( PooledConnection.class );
        when( connection1.isOpen() ).thenReturn( true );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( connection2.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection1 ).thenReturn( connection2 );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        session.run( "RETURN 2" );
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verify( connection2 ).run( eq( "RETURN 2" ), anyParams(), any( Collector.class ) );

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
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        session.run( "RETURN 2" );
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
        verify( connection2 ).run( eq( "RETURN 2" ), anyParams(), any( Collector.class ) );

        verify( connection1, never() ).sync();
        verify( connection1 ).close();
    }

    @Test
    public void closesAndSyncOpenConnectionUsedForRunWhenSessionIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

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
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        session.close();

        verify( connection, never() ).sync();
        verify( connection ).close();
    }

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
        verify( connection1 ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        session.beginTransaction();
        verify( connection1 ).close();
        verify( connectionProvider, times( 2 ) ).acquireConnection( READ );
    }

    @Test
    public void updatesBookmarkWhenTxIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        setBookmark( tx, "TheBookmark" );

        assertNull( session.lastBookmark() );

        tx.close();
        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    public void closesConnectionWhenTxIsClosed()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        tx.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        tx.close();
        verify( connection ).sync();
        verify( connection ).close();
    }

    @Test
    public void ignoresWronglyClosedTx()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( connection1.isOpen() ).thenReturn( true );
        when( connection2.isOpen() ).thenReturn( true );
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
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( connection1.isOpen() ).thenReturn( true );
        when( connection2.isOpen() ).thenReturn( true );
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
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertTrue( tx.isOpen() );

        session.onConnectionError( true );

        assertFalse( tx.isOpen() );
    }

    @Test
    public void markTxToCloseOnUnrecoverableConnectionError()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        Transaction tx = session.beginTransaction();
        assertTrue( tx.isOpen() );

        session.onConnectionError( false );

        assertFalse( tx.isOpen() );
    }

    @Test
    public void closesConnectionWhenResultIsBuffered()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        session.run( "RETURN 1" );
        verify( connectionProvider ).acquireConnection( READ );
        verify( connection ).run( eq( "RETURN 1" ), anyParams(), any( Collector.class ) );

        session.onResultConsumed();

        verify( connection, never() ).sync();
        verify( connection ).close();
    }

    @Test
    public void bookmarkIsPropagatedFromSession()
    {
        String bookmark = "Bookmark";

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ, bookmark );

        try ( Transaction ignore = session.beginTransaction() )
        {
            verify( connection ).run( "BEGIN", singletonMap( "bookmark", value( bookmark ) ), NO_OP );
        }
    }

    @Test
    public void bookmarkIsPropagatedInBeginTransaction()
    {
        String bookmark = "Bookmark";

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        try ( Transaction ignore = session.beginTransaction( bookmark ) )
        {
            verify( connection ).run( "BEGIN", singletonMap( "bookmark", value( bookmark ) ), NO_OP );
        }
    }

    @Test
    public void bookmarkIsPropagatedBetweenTransactions()
    {
        String bookmark1 = "Bookmark1";
        String bookmark2 = "Bookmark2";

        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( connectionProvider.acquireConnection( READ ) ).thenReturn( connection );
        NetworkSession session = newSession( connectionProvider, READ );

        try ( Transaction tx = session.beginTransaction() )
        {
            setBookmark( tx, bookmark1 );
        }

        assertEquals( bookmark1, session.lastBookmark() );

        try ( Transaction tx = session.beginTransaction() )
        {
            verify( connection ).run( "BEGIN", singletonMap( "bookmark", value( bookmark1 ) ), NO_OP );
            assertNull( getBookmark( tx ) );
            setBookmark( tx, bookmark2 );
        }

        assertEquals( bookmark2, session.lastBookmark() );
    }

    @Test
    public void accessModeUsedToAcquireConnections()
    {
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );

        NetworkSession session1 = new NetworkSession( connectionProvider, READ, null, DEV_NULL_LOGGING );
        session1.beginTransaction();
        verify( connectionProvider ).acquireConnection( READ );

        NetworkSession session2 = new NetworkSession( connectionProvider, WRITE, null, DEV_NULL_LOGGING );
        session2.beginTransaction();
        verify( connectionProvider ).acquireConnection( WRITE );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode )
    {
        return newSession( connectionProvider, mode, null );
    }

    private static NetworkSession newSession( ConnectionProvider connectionProvider, AccessMode mode, String bookmark )
    {
        return new NetworkSession( connectionProvider, mode, bookmark, DEV_NULL_LOGGING );
    }

    private static Map<String,Value> anyParams()
    {
        return anyMapOf( String.class, Value.class );
    }

    private static String getBookmark( Transaction tx )
    {
        return ((ExplicitTransaction) tx).bookmark();
    }

    private static void setBookmark( Transaction tx, String bookmark )
    {
        ((ExplicitTransaction) tx).setBookmark( bookmark );
    }
}
