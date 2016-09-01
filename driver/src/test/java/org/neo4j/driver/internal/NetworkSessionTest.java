/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Connection mock = mock( Connection.class );
    private NetworkSession sess = new NetworkSession( mock, new DevNullLogger() );

    @Test
    public void shouldSendAllOnRun() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        NetworkSession sess = new NetworkSession( mock, new DevNullLogger() );

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).flush();
    }

    @Test
    public void shouldNotAllowNewTxWhileOneIsRunning() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        sess.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.beginTransaction();
    }

    @Test
    public void shouldBeAbleToOpenTxAfterPreviousIsClosed() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        sess.beginTransaction().close();

        // When
        Transaction tx = sess.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    public void shouldNotBeAbleToUseSessionWhileOngoingTransaction() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        sess.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.run( "whatever" );
    }

    @Test
    public void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        sess.beginTransaction().close();

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).flush();
    }

    @Test
    public void shouldNotAllowMoreStatementsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( false );

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.run( "whatever" );
    }

    @Test
    public void shouldNotAllowMoreTransactionsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( false );

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.beginTransaction();
    }

    @Test
    public void shouldGetExceptionIfTryingToCloseSessionMoreThanOnce() throws Throwable
    {
        // Given
        NetworkSession sess = new NetworkSession( mock(Connection.class), mock(Logger.class) );
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
