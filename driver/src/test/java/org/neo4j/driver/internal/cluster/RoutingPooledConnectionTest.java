/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.driver.internal.spi.PooledConnection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class RoutingPooledConnectionTest
{
    @Mock
    private PooledConnection pooledConnection;
    @InjectMocks
    private RoutingPooledConnection routingPooledConnection;

    @Test
    public void shouldExposeCreationTimestamp()
    {
        when( pooledConnection.creationTimestamp() ).thenReturn( 42L );

        long timestamp = routingPooledConnection.creationTimestamp();

        assertEquals( 42L, timestamp );
    }

    @Test
    public void shouldExposeLastUsedTimestamp()
    {
        when( pooledConnection.lastUsedTimestamp() ).thenReturn( 42L );

        long timestamp = routingPooledConnection.lastUsedTimestamp();

        assertEquals( 42L, timestamp );
    }
}
