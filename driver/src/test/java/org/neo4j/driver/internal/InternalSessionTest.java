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

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InternalSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldSyncOnRun() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock );

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).sync();
    }

    @Test
    public void shouldNotAllowNewTxWhileOneIsRunning() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock );
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
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock );
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
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock );
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
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock );
        sess.beginTransaction().close();

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).sync();
    }

    @Test
    public void shouldNotAllowMoreStatementsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( false );
        InternalSession sess = new InternalSession( mock );

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.run( "whatever" );
    }

    @Test
    public void shouldNotAllowMoreTransactionsInSessionWhileConnectionClosed() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        when( mock.isOpen() ).thenReturn( false );
        InternalSession sess = new InternalSession( mock );

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.beginTransaction();
    }
}
