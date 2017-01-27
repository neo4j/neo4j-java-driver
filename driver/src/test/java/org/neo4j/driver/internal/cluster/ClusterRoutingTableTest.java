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

import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.EMPTY;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.createClusterComposition;

public class ClusterRoutingTableTest
{
    @Test
    public void shouldReturnStaleIfTtlExpired() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( 1000,
                asList( A, B ), asList( C ), asList( D, E ) ) );
        clock.progress( 1234 );

        // Then
        assertTrue( routingTable.isStale() );
    }

    @Test
    public void shouldReturnStaleIfNoRouter() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( EMPTY, asList( C ), asList( D, E ) ) );

        // Then
        assertTrue( routingTable.isStale() );
    }

    @Test
    public void shouldReturnStaleIfNoReader() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), EMPTY ) );

        // Then
        assertTrue( routingTable.isStale() );
    }


    @Test
    public void shouldReturnStatleIfNoWriter() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), EMPTY, asList( D, E ) ) );

        // Then
        assertTrue( routingTable.isStale() );
    }

    @Test
    public void shouldNotStale() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), asList( D, E ) ) );

        // Then
        assertFalse( routingTable.isStale() );
    }

    @Test
    public void shouldStaleWhenCreate() throws Throwable
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( clock, A );

        // Then
        assertTrue( routingTable.isStale() );
    }
}
