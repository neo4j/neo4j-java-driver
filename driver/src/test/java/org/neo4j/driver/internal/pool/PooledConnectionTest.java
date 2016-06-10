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

import java.util.LinkedList;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.exceptions.DatabaseException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class PooledConnectionTest
{
    @Test
    public void shouldReturnToPoolIfExceptionDuringReset() throws Throwable
    {
        // Given
        final LinkedList<PooledConnection> returnedToPool = new LinkedList<>();
        Connection conn = mock( Connection.class );

        doThrow( new DatabaseException( "asd", "asd" ) ).when(conn).reset( any( StreamCollector.class) );

        PooledConnection pooledConnection = new PooledConnection( conn, new Consumer<PooledConnection>()
        {
            @Override
            public void accept( PooledConnection pooledConnection )
            {
                returnedToPool.add( pooledConnection );
            }
        }, Clock.SYSTEM );

        // When
        pooledConnection.close();

        // Then
        assertThat( returnedToPool, hasItem(pooledConnection) );
        assertThat( returnedToPool.size(), equalTo( 1 ));
    }
}