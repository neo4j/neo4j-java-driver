/*
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

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.InternalException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.exceptions.TLSConnectionException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.exceptions.DatabaseException;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class ConnectionInvalidationTest
{
    private final Connection delegate = mock( Connection.class );

    private final PooledSocketConnection conn = new PooledSocketConnection( delegate, null );

    @Test
    public void shouldInvalidateConnectionWithUnknownAddress() throws Throwable
    {
        when( delegate.boltServerAddress() ).thenReturn( LOCAL_DEFAULT );

        BlockingPooledConnectionQueue queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( false ) );

        PooledConnectionReleaseManager consumer = new PooledConnectionReleaseManager( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @Test
    public void shouldInvalidConnectionIfFailedToReset() throws Throwable
    {
        // Given a connection that's broken
        doThrow( new InvalidOperationException( "That didn't work" ) ).when( delegate ).reset();
        PooledSocketConnection conn = new PooledSocketConnection( delegate, null );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );
        // When/Then
        BlockingPooledConnectionQueue queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseManager consumer = new PooledConnectionReleaseManager( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @Test
    public void shouldInvalidateOnUnrecoverableErrors() throws Throwable
    {
        // When/Then
        assertUnrecoverable( new ServerNeo4jException( new DatabaseException( "Neo.DatabaseError.Whatever", "Hello" ) ) );
        assertUnrecoverable( new InvalidOperationException( "invalid operation" ) );
        assertUnrecoverable( new ConnectionException( "failed to connect" ) );
        assertUnrecoverable( new TLSConnectionException( "failed to connect due to tls" ) );
    }

    @Test
    public void shouldNotInvalidateOnKnownRecoverableErrors() throws Throwable
    {
        assertRecoverable( new ServerNeo4jException( "Neo.ClientError.General.ReadOnly", "Hello, world!" ) );
        assertRecoverable( new ServerNeo4jException( "Neo.TransientError.General.ReadOnly", "Hello, world!" ) );
    }

    @Test
    public void shouldInvalidateOnProtocolViolationExceptions() throws Throwable
    {
        // Server bolt protocol errors
        assertUnrecoverable( new ServerNeo4jException( "Neo.ClientError.Request.InvalidFormat", "Hello, world!" ) );
        assertUnrecoverable( new ServerNeo4jException( "Neo.ClientError.Request.Invalid", "Hello, world!" ) );

        // Driver bolt protocol errors
        assertUnrecoverable( new BoltProtocolException( "bolt protocol error" ) );
    }

    @SuppressWarnings( "unchecked" )
    private void assertUnrecoverable( InternalException exception ) throws Throwable
    {
        doThrow( exception ).when( delegate ).sync();

        // When
        try
        {
            conn.sync();
            fail( "Should've rethrown exception" );
        }
        catch ( InternalException e )
        {
            assertThat( e, equalTo( exception ) );
        }
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );

        // Then
        assertTrue( conn.hasUnrecoverableErrors() );
        BlockingPooledConnectionQueue queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseManager consumer = new PooledConnectionReleaseManager( queue, validator );
        consumer.accept( conn );

        verify( queue, never() ).offer( conn );
    }

    @SuppressWarnings( "unchecked" )
    private void assertRecoverable( InternalException exception ) throws Throwable
    {
        doThrow( exception ).when( delegate ).sync();

        // When
        try
        {
            conn.sync();
            fail( "Should've rethrown exception" );
        }
        catch ( InternalException e )
        {
            assertThat( e, equalTo( exception ) );
        }

        // Then
        assertFalse( conn.hasUnrecoverableErrors() );
        PooledConnectionValidator validator = new PooledConnectionValidator( pool( true ) );
        BlockingPooledConnectionQueue queue = mock( BlockingPooledConnectionQueue.class );
        PooledConnectionReleaseManager consumer = new PooledConnectionReleaseManager( queue, validator );
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
