/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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

        assertEquals( A, routingTable.nextRouter() );
        assertEquals( C, routingTable.nextRouter() );
        assertEquals( D, routingTable.nextRouter() );
        assertEquals( F, routingTable.nextRouter() );
        assertEquals( B, routingTable.nextRouter() );
        assertEquals( E, routingTable.nextRouter() );
        assertEquals( A, routingTable.nextRouter() );
    }

    @Test
    public void shouldPreserveOrderingOfWriters()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        List<BoltServerAddress> writers = asList( D, F, A, C, E );

        routingTable.update( createClusterComposition( EMPTY, writers, EMPTY ) );

        assertEquals( D, routingTable.writers().next() );
        assertEquals( F, routingTable.writers().next() );
        assertEquals( A, routingTable.writers().next() );
        assertEquals( C, routingTable.writers().next() );
        assertEquals( E, routingTable.writers().next() );
        assertEquals( D, routingTable.writers().next() );
    }

    @Test
    public void shouldPreserveOrderingOfReaders()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        List<BoltServerAddress> readers = asList( B, A, F, C, D );

        routingTable.update( createClusterComposition( EMPTY, EMPTY, readers ) );

        assertEquals( B, routingTable.readers().next() );
        assertEquals( A, routingTable.readers().next() );
        assertEquals( F, routingTable.readers().next() );
        assertEquals( C, routingTable.readers().next() );
        assertEquals( D, routingTable.readers().next() );
        assertEquals( B, routingTable.readers().next() );
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

    @Test
    public void shouldReturnCorrectChangeWhenUpdated()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        routingTable.update( createClusterComposition( asList( A, B ), asList( A, C ), asList( C, D ) ) );

        ClusterComposition newComposition =
                createClusterComposition( asList( E, B, F ), asList( E, C ), asList( C, F ) );
        RoutingTableChange change = routingTable.update( newComposition );

        assertEquals( 2, change.added().size() );
        assertThat( change.added(), containsInAnyOrder( E, F ) );
        assertEquals( 2, change.removed().size() );
        assertThat( change.removed(), containsInAnyOrder( A, D ) );
    }

    @Test
    public void shouldNotRemoveServerIfPreWriterNowReader()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        routingTable.update( createClusterComposition( singletonList( A ), singletonList( B ), singletonList( C ) ) );

        ClusterComposition newComposition =
                createClusterComposition( singletonList( D ), singletonList( E ), singletonList( B ) );
        RoutingTableChange change = routingTable.update( newComposition );

        assertEquals( 2, change.added().size() );
        assertThat( change.added(), containsInAnyOrder( D, E ) );
        assertEquals( 2, change.removed().size() );
        assertThat( change.removed(), containsInAnyOrder( A, C ) );
    }

    @Test
    public void shouldReturnNoServersWhenEmpty()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );

        Set<BoltServerAddress> servers = routingTable.servers();

        assertEquals( 0, servers.size() );
    }

    @Test
    public void shouldReturnAllServers()
    {
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        routingTable.update( createClusterComposition( asList( A, B, C ), asList( B, C, D ), asList( C, D, E, F ) ) );

        Set<BoltServerAddress> servers = routingTable.servers();

        assertEquals( new HashSet<>( asList( A, B, C, D, E, F ) ), servers );
    }
}
