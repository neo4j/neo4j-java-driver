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
package org.neo4j.driver.internal.spi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.driver.internal.EventHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.Function;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class StubConnectionPool implements ConnectionPool
{
    public interface EventSink
    {
        EventSink VOID = new Adapter();

        void acquire( BoltServerAddress address, Connection connection );

        void release( BoltServerAddress address, Connection connection );

        void connectionFailure( BoltServerAddress address );

        void purge( BoltServerAddress address, boolean connected );

        void close( Collection<BoltServerAddress> connected );

        class Adapter implements EventSink
        {
            @Override
            public void acquire( BoltServerAddress address, Connection connection )
            {
            }

            @Override
            public void release( BoltServerAddress address, Connection connection )
            {
            }

            @Override
            public void connectionFailure( BoltServerAddress address )
            {
            }

            @Override
            public void purge( BoltServerAddress address, boolean connected )
            {
            }

            @Override
            public void close( Collection<BoltServerAddress> connected )
            {
            }
        }
    }

    private final Clock clock;
    private final EventSink events;
    private final Function<BoltServerAddress,Connection> factory;
    private static final Function<BoltServerAddress,Connection> NULL_FACTORY =
            new Function<BoltServerAddress,Connection>()
            {
                @Override
                public Connection apply( BoltServerAddress boltServerAddress )
                {
                    return null;
                }
            };

    public StubConnectionPool( Clock clock, final EventHandler events, Function<BoltServerAddress,Connection> factory )
    {
        this( clock, events == null ? null : new EventSink()
        {
            @Override
            public void acquire( BoltServerAddress address, Connection connection )
            {
                events.add( new AcquireEvent( Thread.currentThread(), address, connection ) );
            }

            @Override
            public void release( BoltServerAddress address, Connection connection )
            {
                events.add( new ReleaseEvent( Thread.currentThread(), address, connection ) );
            }

            @Override
            public void connectionFailure( BoltServerAddress address )
            {
                events.add( new ConnectionFailureEvent( Thread.currentThread(), address ) );
            }

            @Override
            public void purge( BoltServerAddress address, boolean connected )
            {
                events.add( new PurgeEvent( Thread.currentThread(), address, connected ) );
            }

            @Override
            public void close( Collection<BoltServerAddress> connected )
            {
                events.add( new CloseEvent( Thread.currentThread(), connected ) );
            }
        }, factory );
    }

    public StubConnectionPool( Clock clock, EventSink events, Function<BoltServerAddress,Connection> factory )
    {
        this.clock = clock;
        this.events = events == null ? EventSink.VOID : events;
        this.factory = factory == null ? NULL_FACTORY : factory;
    }

    private enum State
    {
        AVAILABLE,
        CONNECTED,
        PURGED
    }

    private final ConcurrentMap<BoltServerAddress,State> hosts = new ConcurrentHashMap<>();

    public StubConnectionPool up( String host, int port )
    {
        hosts.putIfAbsent( new BoltServerAddress( host, port ), State.AVAILABLE );
        return this;
    }

    public StubConnectionPool down( String host, int port )
    {
        hosts.remove( new BoltServerAddress( host, port ) );
        return this;
    }

    @Override
    public Connection acquire( BoltServerAddress address )
    {
        if ( hosts.replace( address, State.CONNECTED ) == null )
        {
            events.connectionFailure( address );
            throw new ServiceUnavailableException( "Host unavailable: " + address );
        }
        Connection connection = new StubConnection( address, factory.apply( address ), events, clock );
        events.acquire( address, connection );
        return connection;
    }

    @Override
    public void purge( BoltServerAddress address )
    {
        State state = hosts.replace( address, State.PURGED );
        events.purge( address, state == State.CONNECTED );
    }

    @Override
    public boolean hasAddress( BoltServerAddress address )
    {
        return State.CONNECTED == hosts.get( address );
    }

    @Override
    public void close()
    {
        List<BoltServerAddress> connected = new ArrayList<>( hosts.size() );
        for ( Map.Entry<BoltServerAddress,State> entry : hosts.entrySet() )
        {
            if ( entry.getValue() == State.CONNECTED )
            {
                connected.add( entry.getKey() );
            }
        }
        events.close( connected );
        hosts.clear();
    }

    private static class StubConnection extends PooledConnection
    {
        private final BoltServerAddress address;

        StubConnection(
                final BoltServerAddress address,
                final Connection delegate,
                final EventSink events,
                Clock clock )
        {
            super( delegate, new Consumer<PooledConnection>()
            {
                @Override
                public void accept( PooledConnection self )
                {
                    events.release( address, self );
                    if ( delegate != null )
                    {
                        delegate.close();
                    }
                }
            }, clock );
            this.address = address;
        }

        @Override
        public String toString()
        {
            return String.format( "StubConnection{%s}@%s", address, System.identityHashCode( this ) );
        }

        @Override
        public BoltServerAddress boltServerAddress()
        {
            return address;
        }
    }

    public static abstract class Event extends org.neo4j.driver.internal.Event<EventSink>
    {
        final Thread thread;

        private Event( Thread thread )
        {
            this.thread = thread;
        }

        public static Matcher<? extends Event> acquire( String host, int port )
        {
            return acquire(
                    any( Thread.class ),
                    equalTo( new BoltServerAddress( host, port ) ),
                    any( Connection.class ) );
        }

        public static Matcher<? extends Event> acquire( Connection connection )
        {
            return acquire( any( Thread.class ), any( BoltServerAddress.class ), sameInstance( connection ) );
        }

        public static Matcher<? extends Event> acquire(
                final Matcher<Thread> thread,
                final Matcher<BoltServerAddress> address,
                final Matcher<Connection> connection )
        {
            return new TypeSafeMatcher<AcquireEvent>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "acquire event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> of address <" )
                            .appendDescriptionOf( address )
                            .appendText( "> resulting in connection <" )
                            .appendDescriptionOf( connection )
                            .appendText( ">" );
                }

                @Override
                protected boolean matchesSafely( AcquireEvent event )
                {
                    return thread.matches( event.thread ) &&
                            address.matches( event.address ) &&
                            connection.matches( event.connection );
                }
            };
        }

        public static Matcher<? extends Event> release(
                final Matcher<Thread> thread,
                final Matcher<BoltServerAddress> address,
                final Matcher<Connection> connection )
        {
            return new TypeSafeMatcher<ReleaseEvent>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "release event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> of address <" )
                            .appendDescriptionOf( address )
                            .appendText( "> and connection <" )
                            .appendDescriptionOf( connection )
                            .appendText( ">" );
                }

                @Override
                protected boolean matchesSafely( ReleaseEvent event )
                {
                    return thread.matches( event.thread ) &&
                            address.matches( event.address ) &&
                            connection.matches( event.connection );
                }
            };
        }

        public static Matcher<? extends Event> connectionFailure( String host, int port )
        {
            return connectionFailure( any( Thread.class ), equalTo( new BoltServerAddress( host, port ) ) );
        }

        public static Matcher<? extends Event> connectionFailure(
                final Matcher<Thread> thread,
                final Matcher<BoltServerAddress> address )
        {
            return new TypeSafeMatcher<ConnectionFailureEvent>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "connection failure event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> of address <" )
                            .appendDescriptionOf( address )
                            .appendText( ">" );
                }

                @Override
                protected boolean matchesSafely( ConnectionFailureEvent event )
                {
                    return thread.matches( event.thread ) &&
                            address.matches( event.address );
                }
            };
        }

        public static Matcher<? extends Event> purge(
                final Matcher<Thread> thread,
                final Matcher<BoltServerAddress> address,
                final Matcher<Boolean> removed )
        {
            return new TypeSafeMatcher<PurgeEvent>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "purge event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> of address <" )
                            .appendDescriptionOf( address )
                            .appendText( "> resulting in actual removal: " )
                            .appendDescriptionOf( removed );
                }

                @Override
                protected boolean matchesSafely( PurgeEvent event )
                {
                    return thread.matches( event.thread ) &&
                            address.matches( event.address ) &&
                            removed.matches( event.connected );
                }
            };
        }

        public static Matcher<? extends Event> close(
                final Matcher<Thread> thread,
                final Matcher<Collection<BoltServerAddress>> addresses )
        {
            return new TypeSafeMatcher<CloseEvent>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "close event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> resulting in closing connections to <" )
                            .appendDescriptionOf( addresses )
                            .appendText( ">" );
                }

                @Override
                protected boolean matchesSafely( CloseEvent event )
                {
                    return thread.matches( event.thread ) && addresses.matches( event.connected );
                }
            };
        }
    }

    private static class AcquireEvent extends Event
    {
        private final BoltServerAddress address;
        private final Connection connection;

        AcquireEvent( Thread thread, BoltServerAddress address, Connection connection )
        {
            super( thread );
            this.address = address;
            this.connection = connection;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.acquire( address, connection );
        }
    }

    private static class ReleaseEvent extends Event
    {
        private final BoltServerAddress address;
        private final Connection connection;

        ReleaseEvent( Thread thread, BoltServerAddress address, Connection connection )
        {
            super( thread );
            this.address = address;
            this.connection = connection;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.release( address, connection );
        }
    }

    private static class ConnectionFailureEvent extends Event
    {
        private final BoltServerAddress address;

        ConnectionFailureEvent( Thread thread, BoltServerAddress address )
        {
            super( thread );
            this.address = address;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.connectionFailure( address );
        }
    }

    private static class PurgeEvent extends Event
    {
        private final BoltServerAddress address;
        private final boolean connected;

        PurgeEvent( Thread thread, BoltServerAddress address, boolean connected )
        {
            super( thread );
            this.address = address;
            this.connected = connected;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.purge( address, connected );
        }
    }

    private static class CloseEvent extends Event
    {
        private final Collection<BoltServerAddress> connected;

        CloseEvent( Thread thread, Collection<BoltServerAddress> connected )
        {
            super( thread );
            this.connected = connected;
        }

        @Override
        public void dispatch( EventSink sink )
        {
            sink.close( connected );
        }
    }
}
