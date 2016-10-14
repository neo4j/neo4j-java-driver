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
package org.neo4j.driver.internal;


import org.junit.Test;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterViewTest
{

    @Test
    public void shouldRoundRobinAmongRoutingServers()
    {
        // Given
        ClusterView clusterView = new ClusterView( 5L, mock( Clock.class ), mock( Logger.class ) );

        // When
        clusterView.addRouters( asList( address("host1"), address( "host2" ), address( "host3" )));

        // Then
        assertThat(clusterView.nextRouter(), equalTo(address( "host1" )));
        assertThat(clusterView.nextRouter(), equalTo(address( "host2" )));
        assertThat(clusterView.nextRouter(), equalTo(address( "host3" )));
        assertThat(clusterView.nextRouter(), equalTo(address( "host1" )));
    }

    @Test
    public void shouldRoundRobinAmongReadServers()
    {
        // Given
        ClusterView clusterView = new ClusterView( 5L, mock( Clock.class ), mock( Logger.class ) );

        // When
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));

        // Then
        assertThat(clusterView.nextReader(), equalTo(address( "host1" )));
        assertThat(clusterView.nextReader(), equalTo(address( "host2" )));
        assertThat(clusterView.nextReader(), equalTo(address( "host3" )));
        assertThat(clusterView.nextReader(), equalTo(address( "host1" )));
    }

    @Test
    public void shouldRoundRobinAmongWriteServers()
    {
        // Given
        ClusterView clusterView = new ClusterView( 5L, mock( Clock.class ), mock( Logger.class ) );

        // When
        clusterView.addWriters( asList( address("host1"), address( "host2" ), address( "host3" )));

        // Then
        assertThat(clusterView.nextWriter(), equalTo(address( "host1" )));
        assertThat(clusterView.nextWriter(), equalTo(address( "host2" )));
        assertThat(clusterView.nextWriter(), equalTo(address( "host3" )));
        assertThat(clusterView.nextWriter(), equalTo(address( "host1" )));
    }

    @Test
    public void shouldRemoveServer()
    {
        // Given
        ClusterView clusterView = new ClusterView( 5L, mock( Clock.class ), mock( Logger.class ) );

        clusterView.addRouters( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addWriters( asList( address("host2"), address( "host4" )));

        // When
        clusterView.remove( address( "host2" ) );

        // Then
        assertThat(clusterView.routingServers(), containsInAnyOrder(address( "host1" ), address( "host3" )));
        assertThat(clusterView.readServers(), containsInAnyOrder(address( "host1" ), address( "host3" )));
        assertThat(clusterView.writeServers(), containsInAnyOrder(address( "host4" )));
        assertThat(clusterView.all(), containsInAnyOrder( address( "host1" ), address( "host3" ), address( "host4" ) ));
    }

    @Test
    public void shouldBeStaleIfExpired()
    {
        // Given
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 6L );
        ClusterView clusterView = new ClusterView( 5L, clock, mock( Logger.class ) );
        clusterView.addRouters( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addWriters( asList( address("host2"), address( "host4" )));

        // Then
        assertTrue(clusterView.isStale());
    }

    @Test
    public void shouldNotBeStaleIfNotExpired()
    {
        // Given
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 4L );
        ClusterView clusterView = new ClusterView( 5L, clock, mock( Logger.class ) );
        clusterView.addRouters( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addWriters( asList( address("host2"), address( "host4" )));

        // Then
        assertFalse(clusterView.isStale());
    }

    @Test
    public void shouldBeStaleIfOnlyOneRouter()
    {
        // Given
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 4L );
        ClusterView clusterView = new ClusterView( 5L, clock, mock( Logger.class ) );
        clusterView.addRouters( singletonList( address( "host1" ) ) );
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));
        clusterView.addWriters( asList( address("host2"), address( "host4" )));

        // When

        // Then
        assertTrue(clusterView.isStale());
    }

    @Test
    public void shouldBeStaleIfNoReader()
    {
        // Given
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 4L );
        ClusterView clusterView = new ClusterView( 5L, clock, mock( Logger.class ) );
        clusterView.addRouters( singletonList( address( "host1" ) ) );
        clusterView.addWriters( asList( address("host2"), address( "host4" )));

        // Then
        assertTrue(clusterView.isStale());
    }

    @Test
    public void shouldBeStaleIfNoWriter()
    {
        // Given
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 4L );
        ClusterView clusterView = new ClusterView( 5L, clock, mock( Logger.class ) );
        clusterView.addRouters( singletonList( address( "host1" ) ) );
        clusterView.addReaders( asList( address("host1"), address( "host2" ), address( "host3" )));

        // Then
        assertTrue(clusterView.isStale());
    }

    private BoltServerAddress address(String host)
    {
        return new BoltServerAddress( host );
    }

}