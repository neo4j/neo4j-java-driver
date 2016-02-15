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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Collections;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class InternalConnectionPoolTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldThrowExceptionWhenConnectionPoolIsFull() throws Throwable
    {
        // Given
        URI uri = URI.create( "bolt://asd" );
        Connector connector = connector( "bolt" );
        Config config = Config.build().withConnectionPoolSize( 1 ).toConfig();
        InternalConnectionPool pool = new InternalConnectionPool( singletonList( connector ),
                Clock.SYSTEM, config, 100 );

        // When & Then
        pool.acquire( uri );

        exception.expect( ClientException.class );
        exception.expectMessage(
                "Failed to acquire a session with Neo4j as all the connections in the connection pool are already" +
                " occupied by other sessions.");

        pool.acquire( uri );
    }

    @Test
    public void shouldAcquireAndRelease() throws Throwable
    {
        // Given
        URI uri = URI.create( "bolt://asd" );
        Connector connector = connector( "bolt" );
        Config config = Config.defaultConfig();
        InternalConnectionPool pool = new InternalConnectionPool( singletonList( connector ),
                Clock.SYSTEM, config, 100 );

        Connection conn = pool.acquire( uri );
        conn.close();

        // When
        pool.acquire( uri );

        // Then
        verify( connector, times( 1 ) ).connect( uri, config );
    }

    private Connector connector( String scheme )
    {
        Connector mock = mock( Connector.class );
        when( mock.supportedSchemes() ).thenReturn( Collections.singletonList( scheme ) );
        when( mock.connect( any( URI.class ), any( Config.class ) ) ).thenReturn( mock( Connection.class ) );
        return mock;
    }
}
