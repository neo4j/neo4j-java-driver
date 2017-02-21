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

import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.AccessMode.READ;

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
        session = new NetworkSession( connectionProvider, READ, DEV_NULL_LOGGING );
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
    public void shouldNotAllowMoreStatementsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( false );

        // Expect
        exception.expect( ServiceUnavailableException.class );

        // When
        session.run( "whatever" );
    }

    @Test
    public void shouldNotAllowMoreTransactionsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        when( connection.isOpen() ).thenReturn( false );

        // Expect
        exception.expect( ServiceUnavailableException.class );

        // When
        session.beginTransaction();
    }

    @Test
    public void shouldGetExceptionIfTryingToCloseSessionMoreThanOnce() throws Throwable
    {
        // Given
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class, RETURNS_MOCKS );
        NetworkSession sess = new NetworkSession( connectionProvider, READ, DEV_NULL_LOGGING );
        try
        {
            sess.close();
        }
        catch( Exception e )
        {
            fail("Should not get any problem to close first time");
        }

        // When
        try
        {
            sess.close();
            fail( "Should have received an error to close second time" );
        }
        catch( Exception e )
        {
           assertThat( e.getMessage(), equalTo("This session has already been closed." ));
        }
    }
}
