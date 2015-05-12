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

import java.net.URI;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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