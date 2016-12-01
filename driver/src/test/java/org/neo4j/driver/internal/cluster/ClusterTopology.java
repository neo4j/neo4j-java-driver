/*
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
package org.neo4j.driver.internal.cluster;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.internal.Event;
import org.neo4j.driver.internal.EventHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;

import static java.util.Arrays.asList;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.READ;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.ROUTE;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.WRITE;

class ClusterTopology implements ClusterComposition.Provider
{
    public interface EventSink
    {
        EventSink VOID = new Adapter();

        void clusterComposition( BoltServerAddress address, ClusterComposition result );

        class Adapter implements EventSink
        {
            @Override
            public void clusterComposition( BoltServerAddress address, ClusterComposition result )
            {
            }
        }
    }

    private static final List<String> KEYS = Collections.unmodifiableList( asList( "servers", "ttl" ) );
    private final Map<BoltServerAddress,View> views = new HashMap<>();
    private final EventSink events;
    private final Clock clock;

    ClusterTopology( final EventHandler events, Clock clock )
    {
        this( events == null ? null : new EventSink()
        {
            @Override
            public void clusterComposition( BoltServerAddress address, ClusterComposition result )
            {
                events.add( new CompositionRequest( Thread.currentThread(), address, result ) );
            }
        }, clock );
    }

    ClusterTopology( EventSink events, Clock clock )
    {
        this.events = events == null ? EventSink.VOID : events;
        this.clock = clock;
    }

    public View on( String host, int port )
    {
        View view = new View();
        views.put( new BoltServerAddress( host, port ), view );
        return view;
    }

    public enum Role
    {
        READ,
        WRITE,
        ROUTE
    }

    public static class View
    {
        private long ttl = 60_000;
        private final Set<BoltServerAddress> readers = new HashSet<>(),
                writers = new HashSet<>(),
                routers = new HashSet<>();

        public View ttlSeconds( long ttl )
        {
            this.ttl = ttl * 1000;
            return this;
        }

        public View provide( String host, int port, Role... roles )
        {
            for ( Role role : roles )
            {
                servers( role ).add( new BoltServerAddress( host, port ) );
            }
            return this;
        }

        private Set<BoltServerAddress> servers( Role role )
        {
            switch ( role )
            {
            case READ:
                return readers;
            case WRITE:
                return writers;
            case ROUTE:
                return routers;
            default:
                throw new IllegalArgumentException( role.name() );
            }
        }

        ClusterComposition composition( long now )
        {
            return new ClusterComposition( now + ttl, servers( READ ), servers( WRITE ), servers( ROUTE ) );
        }
    }

    @Override
    public ClusterComposition getClusterComposition( Connection connection )
    {
        BoltServerAddress router = connection.boltServerAddress();
        View view = views.get( router );
        ClusterComposition result = view == null ? null : view.composition( clock.millis() );
        events.clusterComposition( router, result );
        return result;
    }

    public static final class CompositionRequest extends Event<EventSink>
    {
        final Thread thread;
        final BoltServerAddress address;
        private final ClusterComposition result;

        private CompositionRequest( Thread thread, BoltServerAddress address, ClusterComposition result )
        {
            this.thread = thread;
            this.address = address;
            this.result = result;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.clusterComposition( address, result );
        }

        public static Matcher<? extends CompositionRequest> clusterComposition(
                final Matcher<Thread> thread,
                final Matcher<BoltServerAddress> address,
                final Matcher<ClusterComposition> result )
        {
            return new TypeSafeMatcher<CompositionRequest>()
            {
                @Override
                protected boolean matchesSafely( CompositionRequest event )
                {
                    return thread.matches( event.thread )
                            && address.matches( event.address )
                            && result.matches( event.result );
                }

                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "a successful cluster composition request on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> from address <" )
                            .appendDescriptionOf( address )
                            .appendText( "> returning <" )
                            .appendDescriptionOf( result )
                            .appendText( ">" );
                }
            };
        }
    }
}
