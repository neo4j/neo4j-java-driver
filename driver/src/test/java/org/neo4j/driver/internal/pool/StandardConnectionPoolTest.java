package org.neo4j.driver.internal.pool;

import junit.framework.TestCase;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class StandardConnectionPoolTest
{
    @Test
    public void shouldAcquireAndRelease() throws Throwable
    {
        // Given
        URI uri = URI.create( "neo4j://asd" );
        Connector connector = connector( "neo4j" );
        StandardConnectionPool pool = new StandardConnectionPool( asList( connector ) );

        Connection conn = pool.acquire( uri );
        conn.close();

        // When
        pool.acquire( uri );

        // Then
        verify( connector, times( 1 ) ).connect( uri );
    }

    private Connector connector(String scheme)
    {
        Connector mock = mock( Connector.class );
        when( mock.supportedSchemes() ).thenReturn( asList( scheme ));
        when( mock.connect( any(URI.class) )).thenReturn( mock(Connection.class) );
        return mock;
    }
}