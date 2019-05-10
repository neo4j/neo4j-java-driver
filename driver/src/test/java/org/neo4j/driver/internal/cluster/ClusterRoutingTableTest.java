/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
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
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

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
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

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
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

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
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

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
        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, clock, A );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"Molly", ABSENT_DB_NAME, "I AM A NAME"} )
    void shouldReturnDatabaseNameCorrectly( String db )
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( db, clock, A );

        // Then
        assertEquals( db, routingTable.database() );
    }

    @Test
    void shouldContainInitialRouters()
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, clock, A, B, C );

        // Then
        assertArrayEquals( new BoltServerAddress[]{A, B, C}, routingTable.routers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfRouters()
    {
        ClusterRoutingTable routingTable = newRoutingTable( new FakeClock() );
        List<BoltServerAddress> routers = asList( A, C, D, F, B, E );

        routingTable.update( createClusterComposition( routers, EMPTY, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{A, C, D, F, B, E}, routingTable.routers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfWriters()
    {
        ClusterRoutingTable routingTable = newRoutingTable( new FakeClock() );
        List<BoltServerAddress> writers = asList( D, F, A, C, E );

        routingTable.update( createClusterComposition( EMPTY, writers, EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{D, F, A, C, E}, routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfReaders()
    {
        ClusterRoutingTable routingTable = newRoutingTable( new FakeClock() );
        List<BoltServerAddress> readers = asList( B, A, F, C, D );

        routingTable.update( createClusterComposition( EMPTY, EMPTY, readers ) );

        assertArrayEquals( new BoltServerAddress[]{B, A, F, C, D}, routingTable.readers().toArray() );
    }

    @Test
    void shouldTreatOneRouterAsValid()
    {
        ClusterRoutingTable routingTable = newRoutingTable( new FakeClock() );

        List<BoltServerAddress> routers = singletonList( A );
        List<BoltServerAddress> writers = asList( B, C );
        List<BoltServerAddress> readers = asList( D, E );

        routingTable.update( createClusterComposition( routers, writers, readers ) );

        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForExpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertTrue( routingTable.isStale( 0 ) );
    }

    @Test
    void shouldNotBeStaleForExpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertFalse( routingTable.isStale( Duration.ofSeconds( 30 ).toMillis() ) );
    }

    private ClusterRoutingTable newRoutingTable( Clock clock )
    {
        return new ClusterRoutingTable( ABSENT_DB_NAME, clock );
    }
}
