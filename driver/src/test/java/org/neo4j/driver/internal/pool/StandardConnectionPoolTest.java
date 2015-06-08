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