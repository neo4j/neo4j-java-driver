/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;

import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ConnectionInvalidationTest
{
    private final Connection delegate = mock( Connection.class );
    private final PooledConnection conn = new PooledConnection( delegate, null );

    @Test
    public void shouldInvalidateConnectionThatIsOld() throws Throwable
    {
        // Given a connection that's broken
        Mockito.doThrow( new ClientException( "That didn't work" ) ).when( delegate ).sync();

        // When/Then
        assertTrue( new PooledConnectionValidator( 10 ).isValid( conn, 1 ) );
        assertFalse(new PooledConnectionValidator( 10 ).isValid( conn, 100 ));
        assertFalse(new PooledConnectionValidator( 10 ).isValid( conn, 10 ));
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

    private void assertUnrecoverable( Neo4jException exception )
    {
        doThrow( exception ).when( delegate ).sync();

        // When
        try
        {
            conn.sync();
            fail("Should've rethrown exception");
        }
        catch( Neo4jException e )
        {
            assertThat(e, equalTo( exception ));
        }

        // Then
        assertTrue( conn.hasUnrecoverableErrors() );
        assertFalse( new PooledConnectionValidator( 100 ).isValid( conn, 1 ) );
    }

    private void assertRecoverable( Neo4jException exception )
    {
        doThrow( exception ).when( delegate ).sync();

        // When
        try
        {
            conn.sync();
            fail("Should've rethrown exception");
        }
        catch( Neo4jException e )
        {
            assertThat(e, equalTo( exception ));
        }

        // Then
        assertFalse( conn.hasUnrecoverableErrors() );
        assertTrue( new PooledConnectionValidator( 100 ).isValid( conn, 1 ) );
    }
}
