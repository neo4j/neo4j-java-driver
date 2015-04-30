package org.neo4j.driver.internal.pool;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOError;
import java.io.IOException;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.spi.Connection;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
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
        // When/Then
        assertFalse(new PooledConnectionValidator( 10 ).isValid( conn, 100 ));
        assertFalse(new PooledConnectionValidator( 10 ).isValid( conn, 10 ));
        assertTrue( new PooledConnectionValidator( 10 ).isValid( conn, 1 ) );
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
