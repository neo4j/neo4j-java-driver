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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static junit.framework.TestCase.assertNotNull;
import static org.jsoup.helper.Validate.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InternalSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Connection mock = mock( Connection.class );
    private InternalSession sess = new InternalSession( mock, new DevNullLogger() );

    @Test
    public void shouldSyncOnRun() throws Throwable
    {
        // Given
        when( mock.isOpen() ).thenReturn( true );
        InternalSession sess = new InternalSession( mock, new DevNullLogger() );

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).sync();
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
        verify( mock ).sync();
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
    public void shouldWarnAndCloseOnLeak() throws Throwable
    {
        // Given
        CloseTrackingConnection connection = new CloseTrackingConnection();
        Logger logger = spy( Logger.class );

        // When
        new InternalSession( connection, logger );

        // Then
        long deadline = System.currentTimeMillis() + 1000 * 30;
        while ( !connection.closeCalled.get() )
        {
            System.gc();
            if ( System.currentTimeMillis() > deadline )
            {
                fail( "Expected unclosed session object to close its inner connection on finalize." );
            }
        }

        verify( logger ).error( "Neo4j Session object leaked, please ensure that your application calls the `close` method on Sessions before disposing of the objects.", null );
    }

    private static class CloseTrackingConnection implements Connection
    {
        AtomicBoolean closeCalled = new AtomicBoolean();

        @Override
        public void init( String clientName )
        {

        }

        @Override
        public void run( String statement, Map<String,Value> parameters, StreamCollector collector )
        {

        }

        @Override
        public void discardAll()
        {

        }

        @Override
        public void pullAll( StreamCollector collector )
        {

        }

        @Override
        public void reset( StreamCollector collector )
        {

        }

        @Override
        public void sync()
        {

        }

        @Override
        public void close()
        {
            closeCalled.set( true );
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }
    }
}
