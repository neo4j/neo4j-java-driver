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

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.driver.internal.EventHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StubConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.MatcherFactory;
import org.neo4j.driver.v1.EventLogger;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.Function;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.READ;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.ROUTE;
import static org.neo4j.driver.internal.cluster.ClusterTopology.Role.WRITE;
import static org.neo4j.driver.internal.spi.StubConnectionPool.Event.acquire;
import static org.neo4j.driver.internal.spi.StubConnectionPool.Event.connectionFailure;
import static org.neo4j.driver.internal.util.FakeClock.Event.sleep;
import static org.neo4j.driver.internal.util.MatcherFactory.inAnyOrder;
import static org.neo4j.driver.internal.util.MatcherFactory.matches;
import static org.neo4j.driver.v1.EventLogger.Entry.message;
import static org.neo4j.driver.v1.EventLogger.Level.INFO;

public class LoadBalancerTest
{
    private static final long RETRY_TIMEOUT_DELAY = 5_000;
    private static final int MAX_ROUTING_FAILURES = 5;
    @Rule
    public final TestRule printEventsOnFailure = new TestRule()
    {
        @Override
        public Statement apply( final Statement base, Description description )
        {
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable e )
                    {
                        events.printEvents( System.err );
                        throw e;
                    }
                }
            };
        }
    };
    private final EventHandler events = new EventHandler();
    private final FakeClock clock = new FakeClock( events, true );
    private final EventLogger log = new EventLogger( events, null, INFO );
    private final StubConnectionPool connections = new StubConnectionPool( clock, events, null );
    private final ClusterTopology cluster = new ClusterTopology( events, clock );

    private LoadBalancer seedLoadBalancer( String host, int port ) throws Exception
    {
        return new LoadBalancer(
                new RoutingSettings( MAX_ROUTING_FAILURES, RETRY_TIMEOUT_DELAY ),
                clock,
                log,
                connections,
                cluster,
                new BoltServerAddress( host, port ) );
    }

    @Test
    public void shouldConnectToRouter() throws Exception
    {
        // given
        connections.up( "some.host", 1337 );
        cluster.on( "some.host", 1337 )
                .provide( "some.host", 1337, READ, WRITE, ROUTE )
                .provide( "another.host", 1337, ROUTE );

        // when
        Connection connection = seedLoadBalancer( "some.host", 1337 ).acquireReadConnection();

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 1 ) );
        events.assertContains( acquiredConnection( "some.host", 1337, connection ) );
    }

    @Test
    public void shouldConnectToRouterOnInitialization() throws Exception
    {
        // given
        connections.up( "some.host", 1337 );
        cluster.on( "some.host", 1337 )
                .provide( "some.host", 1337, READ, WRITE, ROUTE )
                .provide( "another.host", 1337, ROUTE );

        // when
        seedLoadBalancer( "some.host", 1337 );

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 1 ) );
    }

    @Test
    public void shouldReconnectWithRouterAfterTtlExpires() throws Exception
    {
        // given
        coreClusterOn( 20, "some.host", 1337, "another.host" );
        connections.up( "some.host", 1337 ).up( "another.host", 1337 );

        LoadBalancer routing = seedLoadBalancer( "some.host", 1337 );

        // when
        clock.progress( 25_000 );  // will cause TTL timeout
        Connection connection = routing.acquireWriteConnection();

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 2 ) );
        events.assertContains( acquiredConnection( "some.host", 1337, connection ) );
    }

    @Test
    public void shouldNotReconnectWithRouterWithinTtl() throws Exception
    {
        // given
        coreClusterOn( 20, "some.host", 1337, "another.host" );
        connections.up( "some.host", 1337 ).up( "another.host", 1337 );

        LoadBalancer routing = seedLoadBalancer( "some.host", 1337 );

        // when
        clock.progress( 15_000 );  // not enough to cause TTL timeout
        routing.acquireWriteConnection();

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 1 ) );
    }

    @Test
    public void shouldReconnectWithRouterIfOnlyOneRouterIsFound() throws Exception
    {
        // given
        cluster.on( "here", 1337 )
                .ttlSeconds( 20 )
                .provide( "here", 1337, READ, WRITE, ROUTE );
        connections.up( "here", 1337 );

        LoadBalancer routing = seedLoadBalancer( "here", 1337 );

        // when
        routing.acquireReadConnection();

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 2 ) );
    }

    @Test
    public void shouldReconnectWithRouterIfNoReadersAreAvailable() throws Exception
    {
        // given
        cluster.on( "one", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, WRITE, ROUTE )
                .provide( "two", 1337, ROUTE );
        cluster.on( "two", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, READ, WRITE, ROUTE )
                .provide( "two", 1337, READ, ROUTE );
        connections.up( "one", 1337 ).up( "two", 1337 );

        LoadBalancer routing = seedLoadBalancer( "one", 1337 );

        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 1 ) );

        cluster.on( "one", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, READ, WRITE, ROUTE )
                .provide( "two", 1337, READ, ROUTE );

        // when
        routing.acquireWriteConnection(); // we should require the presence of a READER even though we ask for a WRITER

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 2 ) );
    }

    @Test
    public void shouldReconnectWithRouterIfNoWritersAreAvailable() throws Exception
    {
        // given
        cluster.on( "one", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, READ, ROUTE )
                .provide( "two", 1337, READ, WRITE, ROUTE );
        cluster.on( "two", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, READ, ROUTE )
                .provide( "two", 1337, READ, WRITE, ROUTE );
        connections.up( "one", 1337 );

        events.registerHandler( StubConnectionPool.EventSink.class, new StubConnectionPool.EventSink.Adapter()
        {
            @Override
            public void connectionFailure( BoltServerAddress address )
            {
                connections.up( "two", 1337 );
            }
        } );

        LoadBalancer routing = seedLoadBalancer( "one", 1337 );

        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 1 ) );

        cluster.on( "one", 1337 )
                .ttlSeconds( 20 )
                .provide( "one", 1337, READ, WRITE, ROUTE )
                .provide( "two", 1337, READ, ROUTE );

        // when
        routing.acquireWriteConnection();

        // then
        events.assertCount( any( ClusterTopology.CompositionRequest.class ), equalTo( 2 ) );
    }

    @Test
    public void shouldDropRouterUnableToPerformRoutingTask() throws Exception
    {
        // given
        connections.up( "some.host", 1337 )
                .up( "other.host", 1337 )
                .up( "another.host", 1337 );
        cluster.on( "some.host", 1337 )
                .ttlSeconds( 20 )
                .provide( "some.host", 1337, READ, WRITE, ROUTE )
                .provide( "other.host", 1337, READ, ROUTE );
        cluster.on( "another.host", 1337 )
                .ttlSeconds( 20 )
                .provide( "some.host", 1337, READ, WRITE, ROUTE )
                .provide( "another.host", 1337, READ, ROUTE );
        events.registerHandler( ClusterTopology.EventSink.class, new ClusterTopology.EventSink.Adapter()
        {
            @Override
            public void clusterComposition( BoltServerAddress address, ClusterComposition result )
            {
                if ( result == null )
                {
                    connections.up( "some.host", 1337 );
                    cluster.on( "some.host", 1337 )
                            .ttlSeconds( 20 )
                            .provide( "some.host", 1337, READ, WRITE, ROUTE )
                            .provide( "another.host", 1337, READ, ROUTE );
                }
            }
        } );

        LoadBalancer routing = seedLoadBalancer( "some.host", 1337 );

        // when
        connections.down( "some.host", 1337 );
        clock.progress( 25_000 ); // will cause TTL timeout
        Connection connection = routing.acquireWriteConnection();

        // then
        events.assertCount(
                message(
                        equalTo( INFO ),
                        equalTo( "Server <other.host:1337> unable to perform routing capability, "
                                + "dropping from list of routers." ) ),
                equalTo( 1 ) );
        events.assertContains( acquiredConnection( "some.host", 1337, connection ) );
    }

    @Test
    public void shouldConnectToRoutingServersInTimeoutOrder() throws Exception
    {
        // given
        coreClusterOn( 20, "one", 1337, "two", "tre" );
        connections.up( "one", 1337 );
        events.registerHandler( StubConnectionPool.EventSink.class, new StubConnectionPool.EventSink.Adapter()
        {
            int failed;

            @Override
            public void connectionFailure( BoltServerAddress address )
            {
                if ( ++failed >= 9 ) // three times per server
                {
                    for ( String host : new String[] {"one", "two", "tre"} )
                    {
                        connections.up( host, 1337 );
                    }
                }
            }
        } );

        LoadBalancer routing = seedLoadBalancer( "one", 1337 );

        // when
        connections.down( "one", 1337 );
        clock.progress( 25_000 ); // will cause TTL timeout
        routing.acquireWriteConnection();

        // then
        MatcherFactory<StubConnectionPool.Event> failedAttempts = inAnyOrder(
                connectionFailure( "one", 1337 ),
                connectionFailure( "two", 1337 ),
                connectionFailure( "tre", 1337 ) );
        events.assertContains(
                failedAttempts,
                matches( sleep( RETRY_TIMEOUT_DELAY ) ),
                failedAttempts,
                matches( sleep( 2 * RETRY_TIMEOUT_DELAY ) ),
                failedAttempts,
                matches( sleep( 4 * RETRY_TIMEOUT_DELAY ) ),
                matches( ClusterTopology.CompositionRequest.clusterComposition(
                        any( Thread.class ),
                        any( BoltServerAddress.class ),
                        any( ClusterComposition.class ) ) ) );
    }

    @Test
    public void shouldFailIfEnoughConnectionAttemptsFail() throws Exception
    {
        // when
        try
        {
            seedLoadBalancer( "one", 1337 );
            fail( "expected failure" );
        }
        // then
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
        }
    }

    private static final Function<LoadBalancer,Connection> READ_SERVERS = new Function<LoadBalancer,Connection>()
    {
        @Override
        public Connection apply( LoadBalancer routing )
        {
            return routing.acquireReadConnection();
        }
    };

    @Test
    public void shouldRoundRobinAmongReadServers() throws Exception
    {
        shouldRoundRobinAmong( READ_SERVERS );
    }

    private static final Function<LoadBalancer,Connection> WRITE_SERVERS = new Function<LoadBalancer,Connection>()
    {
        @Override
        public Connection apply( LoadBalancer routing )
        {
            return routing.acquireWriteConnection();
        }
    };

    @Test
    public void shouldRoundRobinAmongWriteServers() throws Exception
    {
        shouldRoundRobinAmong( WRITE_SERVERS );
    }

    private void shouldRoundRobinAmong( Function<LoadBalancer,Connection> acquire ) throws Exception
    {
        // given
        for ( String host : new String[] {"one", "two", "tre"} )
        {
            connections.up( host, 1337 );
            cluster.on( host, 1337 )
                    .ttlSeconds( 20 )
                    .provide( "one", 1337, READ, WRITE, ROUTE )
                    .provide( "two", 1337, READ, WRITE, ROUTE )
                    .provide( "tre", 1337, READ, WRITE, ROUTE );
        }
        LoadBalancer routing = seedLoadBalancer( "one", 1337 );

        // when
        Connection a = acquire.apply( routing );
        Connection b = acquire.apply( routing );
        Connection c = acquire.apply( routing );
        assertNotEquals( a.boltServerAddress(), b.boltServerAddress() );
        assertNotEquals( b.boltServerAddress(), c.boltServerAddress() );
        assertNotEquals( c.boltServerAddress(), a.boltServerAddress() );
        assertEquals( a.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );
        assertEquals( b.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );
        assertEquals( c.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );
        assertEquals( a.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );
        assertEquals( b.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );
        assertEquals( c.boltServerAddress(), acquire.apply( routing ).boltServerAddress() );

        // then
        MatcherFactory<StubConnectionPool.Event> acquireConnections =
                inAnyOrder( acquire( "one", 1337 ), acquire( "two", 1337 ), acquire( "tre", 1337 ) );
        events.assertContains( acquireConnections, acquireConnections, acquireConnections );
        events.assertContains( inAnyOrder( acquire( a ), acquire( b ), acquire( c ) ) );
    }

    @Test
    public void shouldRoundRobinAmongRouters() throws Exception
    {
        // given
        coreClusterOn( 20, "one", 1337, "two", "tre" );
        connections.up( "one", 1337 ).up( "two", 1337 ).up( "tre", 1337 );

        // when
        LoadBalancer routing = seedLoadBalancer( "one", 1337 );
        for ( int i = 1; i < 9; i++ )
        {
            clock.progress( 25_000 );
            routing.acquireReadConnection();
        }

        // then
        final List<String> hosts = new ArrayList<>();
        events.forEach( new ClusterTopology.EventSink()
        {
            @Override
            public void clusterComposition( BoltServerAddress address, ClusterComposition result )
            {
                hosts.add( address.host() );
            }
        } );
        assertEquals( 9, hosts.size() );
        assertEquals( hosts.get( 0 ), hosts.get( 3 ) );
        assertEquals( hosts.get( 1 ), hosts.get( 4 ) );
        assertEquals( hosts.get( 2 ), hosts.get( 5 ) );
        assertEquals( hosts.get( 0 ), hosts.get( 6 ) );
        assertEquals( hosts.get( 1 ), hosts.get( 7 ) );
        assertEquals( hosts.get( 2 ), hosts.get( 8 ) );
        assertNotEquals( hosts.get( 0 ), hosts.get( 1 ) );
        assertNotEquals( hosts.get( 1 ), hosts.get( 2 ) );
        assertNotEquals( hosts.get( 2 ), hosts.get( 0 ) );
    }

    @Test
    public void shouldForgetPreviousServersOnRerouting() throws Exception
    {
        // given
        connections.up( "one", 1337 )
                .up( "two", 1337 );
        cluster.on( "one", 1337 )
                .ttlSeconds( 20 )
                .provide( "bad", 1337, READ, WRITE, ROUTE )
                .provide( "one", 1337, READ, ROUTE );

        LoadBalancer routing = seedLoadBalancer( "one", 1337 );

        // when
        coreClusterOn( 20, "one", 1337, "two" );
        clock.progress( 25_000 ); // will cause TTL timeout
        Connection ra = routing.acquireReadConnection();
        Connection rb = routing.acquireReadConnection();
        Connection w = routing.acquireWriteConnection();
        assertNotEquals( ra.boltServerAddress(), rb.boltServerAddress() );
        assertEquals( ra.boltServerAddress(), routing.acquireReadConnection().boltServerAddress() );
        assertEquals( rb.boltServerAddress(), routing.acquireReadConnection().boltServerAddress() );
        assertEquals( w.boltServerAddress(), routing.acquireWriteConnection().boltServerAddress() );
        assertEquals( ra.boltServerAddress(), routing.acquireReadConnection().boltServerAddress() );
        assertEquals( rb.boltServerAddress(), routing.acquireReadConnection().boltServerAddress() );
        assertEquals( w.boltServerAddress(), routing.acquireWriteConnection().boltServerAddress() );

        // then
        events.assertNone( acquire( "bad", 1337 ) );
    }

    @Test
    public void shouldFailIfNoRouting() throws Exception
    {
        // given
        connections.up( "one", 1337 );
        cluster.on( "one", 1337 )
                .provide( "one", 1337, READ, WRITE );

        // when
        try
        {
            seedLoadBalancer( "one", 1337 );
            fail( "expected failure" );
        }
        // then
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
        }
    }

    @Test
    public void shouldFailIfNoWriting() throws Exception
    {
        // given
        connections.up( "one", 1337 );
        cluster.on( "one", 1337 )
                .provide( "one", 1337, READ, ROUTE );

        // when
        try
        {
            seedLoadBalancer( "one", 1337 );
            fail( "expected failure" );
        }
        // then
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
        }
    }

    @Test
    public void shouldNotForgetAddressForRoutingPurposesWhenUnavailableForOtherUse() throws Exception
    {
        // given
        cluster.on( "one", 1337 )
                .provide( "one", 1337, READ, ROUTE )
                .provide( "two", 1337, WRITE, ROUTE );
        cluster.on( "two", 1337 )
                .provide( "one", 1337, READ, ROUTE )
                .provide( "two", 1337, WRITE, ROUTE );
        connections.up( "one", 1337 );

        LoadBalancer routing = seedLoadBalancer( "one", 1337 );
        connections.down( "one", 1337 );
        events.registerHandler( FakeClock.EventSink.class, new FakeClock.EventSink.Adapter()
        {
            @Override
            public void sleep( long timestamp, long millis )
            {
                connections.up( "two", 1337 );
            }
        } );

        // when
        Connection connection = routing.acquireWriteConnection();

        // then
        assertEquals( new BoltServerAddress( "two", 1337 ), connection.boltServerAddress() );
        events.printEvents( System.out );
    }

    private void coreClusterOn( int ttlSeconds, String leader, int port, String... others )
    {
        for ( int i = 0; i <= others.length; i++ )
        {
            String host = (i == others.length) ? leader : others[i];
            ClusterTopology.View view = cluster.on( host, port )
                    .ttlSeconds( ttlSeconds )
                    .provide( leader, port, READ, WRITE, ROUTE );
            for ( String other : others )
            {
                view.provide( other, port, READ, ROUTE );
            }
        }
    }

    private Matcher<? extends StubConnectionPool.Event> acquiredConnection(
            String host, int port, Connection connection )
    {
        return acquire(
                any( Thread.class ),
                equalTo( new BoltServerAddress( host, port ) ),
                sameInstance( connection ) );
    }
}
