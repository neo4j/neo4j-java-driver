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
package org.neo4j.driver.internal.net.pooling;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumers;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class ConnectionInvalidationTest
{
    private final Connection delegate = mock( Connection.class );
    private final Clock clock = mock( Clock.class );

    private final PooledConnection conn =
            new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), Clock.SYSTEM );

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotInvalidateConnectionThatIsUnableToRun() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) )
                .when( delegate ).run( anyString(), anyMap(), any( Collector.class ) );
        PooledConnection conn = new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), clock );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );

        // When/Then
        BlockingPooledConnectionQueue
                queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue,validator );
        consumer.accept( conn );

        verify( queue ).offer( conn );
    }

    @Test
    public void shouldInvalidateConnectionWithUnknownAddress()
    {
        when( delegate.boltServerAddress() ).thenReturn( LOCAL_DEFAULT );

        BlockingPooledConnectionQueue queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( false ) );

        PooledConnectionReleaseConsumer consumer = new PooledConnectionReleaseConsumer( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @Test
    public void shouldInvalidConnectionIfFailedToReset() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) ).when( delegate ).reset();
        PooledConnection conn = new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), clock );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );
        // When/Then
        BlockingPooledConnectionQueue
                queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @Test
    public void shouldInvalidateOnUnrecoverableProblems() throws Throwable
    {
        // When/Then
        assertUnrecoverable( new ClientException( "Hello, world!", new IOException() ) );
        assertUnrecoverable( new ClientException( "Hello, world!" ) );
    }

    @Test
    public void shouldNotInvalidateOnKnownRecoverableExceptions() throws Throwable
    {
        assertRecoverable( new ClientException( "Neo.ClientError.General.ReadOnly", "Hello, world!" ) );
        assertRecoverable( new TransientException( "Neo.TransientError.General.ReadOnly", "Hello, world!" ) );
    }

    @Test
    public void shouldInvalidateOnProtocolViolationExceptions() throws Throwable
    {
        assertUnrecoverable( new ClientException( "Neo.ClientError.Request.InvalidFormat", "Hello, world!" ) );
        assertUnrecoverable( new ClientException( "Neo.ClientError.Request.Invalid", "Hello, world!" ) );
    }

    @SuppressWarnings( "unchecked" )
    private void assertUnrecoverable( Neo4jException exception )
    {
        doThrow( exception ).when( delegate )
                .run( eq( "assert unrecoverable" ), anyMap(), any( Collector.class ) );

        // When
        try
        {
            conn.run( "assert unrecoverable", new HashMap<String,Value>(), Collector.NO_OP );
            fail( "Should've rethrown exception" );
        }
        catch ( Neo4jException e )
        {
            assertThat( e, equalTo( exception ) );
        }
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );

        // Then
        assertTrue( conn.hasUnrecoverableErrors() );
        BlockingPooledConnectionQueue
                queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @SuppressWarnings( "unchecked" )
    private void assertRecoverable( Neo4jException exception )
    {
        doThrow( exception ).when( delegate ).run( eq( "assert recoverable" ), anyMap(), any( Collector.class ) );

        // When
        try
        {
            conn.run( "assert recoverable", new HashMap<String,Value>(), Collector.NO_OP );
            fail( "Should've rethrown exception" );
        }
        catch ( Neo4jException e )
        {
            assertThat( e, equalTo( exception ) );
        }

        // Then
        assertFalse( conn.hasUnrecoverableErrors() );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );
        BlockingPooledConnectionQueue
                queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, validator );
        consumer.accept( conn );

        verify( queue ).offer( conn );
    }

    private ConnectionPool pool( boolean hasAddress )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.hasAddress( any( BoltServerAddress.class ) ) ).thenReturn( hasAddress );
        return pool;
    }
}
