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

import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.EMPTY;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.F;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.createClusterComposition;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

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
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
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
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    public void shouldBeStaleForReadsButNotWritesWhenNoReaders() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), EMPTY ) );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    public void shouldBeStaleForWritesButNotReadsWhenNoWriters() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), EMPTY, asList( D, E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    public void shouldBeNotStaleWithReadersWritersAndRouters() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = new ClusterRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), asList( D, E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    public void shouldBeStaleForReadsAndWritesAfterCreation() throws Throwable
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( clock, A );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    public void shouldPreserveOrderingOfRouters()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        List<BoltServerAddress> routers = asList( A, C, D, F, B, E );

        routingTable.update( createClusterComposition( routers, EMPTY, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{A, C, D, F, B, E}, routingTable.routers().toArray() );
    }

    @Test
    public void shouldPreserveOrderingOfWriters()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        List<BoltServerAddress> writers = asList( D, F, A, C, E );

        routingTable.update( createClusterComposition( EMPTY, writers, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{D, F, A, C, E}, routingTable.writers().toArray() );
    }

    @Test
    public void shouldPreserveOrderingOfReaders()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        List<BoltServerAddress> readers = asList( B, A, F, C, D );

        routingTable.update( createClusterComposition( EMPTY, EMPTY, readers ) );

        assertArrayEquals( new BoltServerAddress[]{B, A, F, C, D}, routingTable.readers().toArray() );
    }

    @Test
    public void shouldTreatOneRouterAsValid()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );

        List<BoltServerAddress> routers = singletonList( A );
        List<BoltServerAddress> writers = asList( B, C );
        List<BoltServerAddress> readers = asList( D, E );

        routingTable.update( createClusterComposition( routers, writers, readers ) );

        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }
}
