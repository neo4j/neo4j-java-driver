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
package org.neo4j.driver.internal.pool;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumers;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.lang.String.format;
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

public class ConnectionInvalidationTest
{
    private final Connection delegate = mock( Connection.class );
    Clock clock = mock( Clock.class );

    private final PooledConnection conn =
            new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), Clock.SYSTEM );

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldInvalidateConnectionThatIsOld() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) )
                .when( delegate ).run( anyString(), anyMap(), any( StreamCollector.class ) );
        Config config = Config.defaultConfig();
        when( clock.millis() ).thenReturn( 0L, config.idleTimeBeforeConnectionTest() + 1L );
        PooledConnection conn = new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), clock );

        // When/Then
        BlockingQueue<PooledConnection> queue = mock( BlockingQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, new AtomicBoolean( false ), config );
        consumer.accept( conn );

        verify( queue, never() ).add( conn );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotInvalidateConnectionThatIsNotOld() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) )
                .when( delegate ).run( anyString(), anyMap(), any( StreamCollector.class ) );
        Config config = Config.defaultConfig();
        when( clock.millis() ).thenReturn( 0L, config.idleTimeBeforeConnectionTest() - 1L );
        PooledConnection conn = new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), clock );

        // When/Then
        BlockingQueue<PooledConnection> queue = mock( BlockingQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, new AtomicBoolean( false ), config );
        consumer.accept( conn );

        verify( queue ).offer( conn );
    }

    @Test
    public void shouldInvalidConnectionIfFailedToReset() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) ).when( delegate ).reset();
        Config config = Config.defaultConfig();
        PooledConnection conn = new PooledConnection( delegate, Consumers.<PooledConnection>noOp(), clock );

        // When/Then
        BlockingQueue<PooledConnection> queue = mock( BlockingQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, new AtomicBoolean( false ), config );
        consumer.accept( conn );

        verify( queue, never() ).add( conn );
    }

    @Test
    public void shouldInvalidateOnUnrecoverableProblems() throws Throwable
    {
        // When/Then
        assertUnrecoverable( new ClientException( "Hello, world!", new IOException() ) );
        assertUnrecoverable( new ClientException( "Hello, world!" ) );
    }

    @Test
    public void shouldInvalidateOnUnrecoverableClusterErrors() throws Throwable
    {
        // Given
        String clientErrorMessage = format( "Failed to run a statement on a 3.1+ Neo4j core-edge cluster server. %n" +
                        "Please retry the statement if you have defined your own routing strategy to route driver messages to the cluster. " +
                        "Note: 1.0 java driver does not support routing statements to a 3.1+ Neo4j core edge cluster. " +
                        "Please upgrade to 1.1 driver and use `bolt+routing` scheme to route and run statements " +
                        "directly to a 3.1+ core edge cluster." );
        ClientException serverException = new ClientException( "Neo.ClientError.Cluster.NotALeader", "Hello, world!" );
        ClientException clientException = new ClientException( clientErrorMessage, serverException );

        // When/Then
        assertUnrecoverable( serverException, clientException );

        // Given
        serverException = new ClientException( "Neo.TransientError.Cluster.NoLeaderAvailable", "Hello, world" );
        clientException = new ClientException( clientErrorMessage, serverException );

        // When/Then
        assertUnrecoverable( serverException, clientException );
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

    private void assertUnrecoverable( Neo4jException exception )
    {
        assertUnrecoverable( exception, exception );
    }

    @SuppressWarnings( "unchecked" )
    private void assertUnrecoverable( Neo4jException exception, Neo4jException expectingException )
    {
        doThrow( exception ).when( delegate )
                .run( eq("assert unrecoverable"), anyMap(), any( StreamCollector.class ) );

        // When
        try
        {
            conn.run( "assert unrecoverable", new HashMap<String,Value>( ), StreamCollector.NO_OP );
            fail( "Should've rethrown exception" );
        }
        catch ( Neo4jException e )
        {
            assertThat( e.getMessage(), equalTo( expectingException.getMessage() ) );
            assertThat( e.getCause(), equalTo( expectingException.getCause() ) );
        }

        // Then
        assertTrue( conn.hasUnrecoverableErrors() );
        BlockingQueue<PooledConnection> queue = mock( BlockingQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, new AtomicBoolean( false ), Config.defaultConfig() );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @SuppressWarnings( "unchecked" )
    private void assertRecoverable( Neo4jException exception )
    {
        doThrow( exception ).when( delegate ).run( eq("assert recoverable"), anyMap(), any( StreamCollector.class ) );

        // When
        try
        {
            conn.run( "assert recoverable", new HashMap<String,Value>( ), StreamCollector.NO_OP );
            fail( "Should've rethrown exception" );
        }
        catch ( Neo4jException e )
        {
            assertThat( e, equalTo( exception ) );
        }

        // Then
        assertFalse( conn.hasUnrecoverableErrors() );
        BlockingQueue<PooledConnection> queue = mock( BlockingQueue.class );
        PooledConnectionReleaseConsumer consumer =
                new PooledConnectionReleaseConsumer( queue, new AtomicBoolean( false ), Config.defaultConfig() );
        consumer.accept( conn );

        verify( queue ).offer( conn );
    }
}
