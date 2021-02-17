/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.EMPTY;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.createClusterComposition;

class ClusterRoutingTableTest
{
    @Test
    void shouldReturnStaleIfTtlExpired()
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

        // When
        routingTable.update( createClusterComposition( 1000,
                asList( A, B ), asList( C ), asList( D, E ) ) );
        clock.progress( 1234 );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldReturnStaleIfNoRouter()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( createClusterComposition( EMPTY, asList( C ), asList( D, E ) ) );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForReadsButNotWritesWhenNoReaders()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), EMPTY ) );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForWritesButNotReadsWhenNoWriters()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( createClusterComposition( asList( A, B ), EMPTY, asList( D, E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeNotStaleWithReadersWritersAndRouters()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( createClusterComposition( asList( A, B ), asList( C ), asList( D, E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForReadsAndWritesAfterCreation()
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), clock, A );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"Molly", "", "I AM A NAME"} )
    void shouldReturnDatabaseNameCorrectly( String db )
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( database( db ), clock, A );

        // Then
        assertEquals( db, routingTable.database().description() );
    }

    @Test
    void shouldContainInitialRouters()
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), clock, A, B, C );

        // Then
        assertArrayEquals( new BoltServerAddress[]{A, B, C}, routingTable.routers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfRouters()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> routers = asList( A, C, D, F, B, E );

        routingTable.update( createClusterComposition( routers, EMPTY, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{A, C, D, F, B, E}, routingTable.routers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfWriters()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> writers = asList( D, F, A, C, E );

        routingTable.update( createClusterComposition( EMPTY, writers, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{D, F, A, C, E}, routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfReaders()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> readers = asList( B, A, F, C, D );

        routingTable.update( createClusterComposition( EMPTY, EMPTY, readers ) );

        assertArrayEquals( new BoltServerAddress[]{B, A, F, C, D}, routingTable.readers().toArray() );
    }

    @Test
    void shouldTreatOneRouterAsValid()
    {
        ClusterRoutingTable routingTable = newRoutingTable();

        List<BoltServerAddress> routers = singletonList( A );
        List<BoltServerAddress> writers = asList( B, C );
        List<BoltServerAddress> readers = asList( D, E );

        routingTable.update( createClusterComposition( routers, writers, readers ) );

        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldHaveBeStaleForExpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertTrue( routingTable.hasBeenStaleFor( 0 ) );
    }

    @Test
    void shouldNotHaveBeStaleForUnexpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertFalse( routingTable.hasBeenStaleFor( Duration.ofSeconds( 30 ).toMillis() ) );
    }

    @Test
    void shouldDefaultToPreferInitialRouter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        assertTrue( routingTable.preferInitialRouter() );
    }

    @Test
    void shouldPreferInitialRouterIfNoWriter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        routingTable.update( createClusterComposition( EMPTY, EMPTY, EMPTY ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( singletonList( A ), EMPTY, singletonList( A ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( asList( A, B ), EMPTY, asList( A, B ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( EMPTY, EMPTY, singletonList( A ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( singletonList( A ), EMPTY, EMPTY ) );
        assertTrue( routingTable.preferInitialRouter() );
    }

    @Test
    void shouldNotPreferInitialRouterIfHasWriter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        routingTable.update( createClusterComposition( EMPTY, singletonList( A ), EMPTY ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( singletonList( A ), singletonList( A ), singletonList( A ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( asList( A, B ), singletonList( A ), asList( A, B ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( EMPTY, singletonList( A ), singletonList( A ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( createClusterComposition( singletonList( A ), singletonList( A ), EMPTY ) );
        assertFalse( routingTable.preferInitialRouter() );
    }

    private ClusterRoutingTable newRoutingTable()
    {
        return new ClusterRoutingTable( defaultDatabase(), new FakeClock() );
    }

    private ClusterRoutingTable newRoutingTable( Clock clock )
    {
        return new ClusterRoutingTable( defaultDatabase(), clock );
    }
}
