/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.pool;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.spi.Connection;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectionInvalidationTest
{
    private final Connection delegate = mock( Connection.class );
    private final PooledConnection conn = new PooledConnection( delegate, null );

    @Test
    public void shouldInvalidateConnectionThatIsOld() throws Throwable
    {
        // Given a connection that's broken
        doThrow(new ClientException( "That didn't work" )).when( delegate ).sync();

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
