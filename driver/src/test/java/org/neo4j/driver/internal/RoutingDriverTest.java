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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;
import static org.neo4j.driver.v1.Values.value;

public class RoutingDriverTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final BoltServerAddress SEED = new BoltServerAddress( "localhost", 7687 );
    private static final String GET_SERVERS = "CALL dbms.cluster.routing.getServers";
    private static final List<String> NO_ADDRESSES = Collections.emptyList();
    private final ConnectionPool pool = pool();

    @Test
    public void shouldDoRoutingOnInitialization()
    {
        // Given
        final Session session = mock( Session.class );
        when( session.run( GET_SERVERS ) ).thenReturn(
                getServers( singletonList( "localhost:1111" ),
                        singletonList( "localhost:2222" ),
                        singletonList( "localhost:3333" ) ) );

        // When
        RoutingDriver routingDriver = forSession( session );

        // Then
        assertThat( routingDriver.routingServers(),
                containsInAnyOrder( boltAddress( "localhost", 1111 )) );
        assertThat( routingDriver.readServers(),
                containsInAnyOrder( boltAddress( "localhost", 2222 ) ) );
        assertThat( routingDriver.writeServers(),
                containsInAnyOrder( boltAddress( "localhost", 3333 ) ) );

    }

    @Test
    public void shouldDoReRoutingOnSessionAcquisitionIfNecessary()
    {
        // Given
        final Session session = mock( Session.class );
        when( session.run( GET_SERVERS ) )
                .thenReturn(
                        getServers( singletonList( "localhost:1111" ), NO_ADDRESSES, NO_ADDRESSES ) )
                .thenReturn(
                        getServers( singletonList( "localhost:1112" ),
                                singletonList( "localhost:2222" ),
                                singletonList( "localhost:3333" ) ) );

        RoutingDriver routingDriver = forSession( session );

        assertThat( routingDriver.routingServers(),
                containsInAnyOrder( boltAddress( "localhost", 1111 )) );
        assertThat( routingDriver.readServers(), Matchers.<BoltServerAddress>empty() );
        assertThat( routingDriver.writeServers(), Matchers.<BoltServerAddress>empty() );


        // When
        routingDriver.session( AccessMode.READ );

        // Then
        assertThat( routingDriver.routingServers(),
                containsInAnyOrder( boltAddress( "localhost", 1112 ) ));
        assertThat( routingDriver.readServers(),
                containsInAnyOrder( boltAddress( "localhost", 2222 ) ) );
        assertThat( routingDriver.writeServers(),
                containsInAnyOrder( boltAddress( "localhost", 3333 ) ) );
    }

    @Test
    public void shouldNotDoReRoutingOnSessionAcquisitionIfNotNecessary()
    {
        // Given
        final Session session = mock( Session.class );
        when( session.run( GET_SERVERS ) )
                .thenReturn(
                        getServers( asList( "localhost:1111", "localhost:1112", "localhost:1113" ),
                                singletonList( "localhost:2222" ),
                                singletonList( "localhost:3333" ) ) )
                .thenReturn(
                        getServers( singletonList( "localhost:5555" ), NO_ADDRESSES, NO_ADDRESSES ) );

        RoutingDriver routingDriver = forSession( session );

        // When
        routingDriver.session( AccessMode.WRITE );

        // Then
        assertThat( routingDriver.routingServers(),
                not( hasItem( boltAddress( "localhost", 5555 ) ) ) );
    }

    @Test
    public void shouldFailIfNoRouting()
    {
        // Given
        final Session session = mock( Session.class );
        when( session.run( GET_SERVERS ) )
                .thenThrow(
                        new ClientException( "Neo.ClientError.Procedure.ProcedureNotFound", "Procedure not found" ) );

        // Expect
        exception.expect( ServiceUnavailableException.class );

        // When
        forSession( session );
    }

    @Test
    public void shouldForgetAboutServersOnRerouting()
    {
        // Given
        final Session session = mock( Session.class );
        when( session.run( GET_SERVERS ) )
                .thenReturn(
                        getServers( singletonList( "localhost:1111" ), NO_ADDRESSES, NO_ADDRESSES ) )
                .thenReturn(
                        getServers( singletonList( "localhost:1112" ),
                                singletonList( "localhost:2222" ),
                                singletonList( "localhost:3333" ) ) );

        RoutingDriver routingDriver = forSession( session );

        assertThat( routingDriver.routingServers(),
                containsInAnyOrder( boltAddress( "localhost", 1111 )) );


        // When
        routingDriver.session( AccessMode.READ );

        // Then
        assertThat( routingDriver.routingServers(),
                containsInAnyOrder( boltAddress( "localhost", 1112 ) ));
        verify( pool ).purge( boltAddress( "localhost", 1111 ) );
    }

    @Test
    public void shouldRediscoverOnTimeout()
    {
        // Given
        final Session session = mock( Session.class );
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 0L, 11000L, 22000L );
        when( session.run( GET_SERVERS ) )
                .thenReturn(
                        getServers( asList( "localhost:1111", "localhost:1112", "localhost:1113" ),
                                singletonList( "localhost:2222" ),
                                singletonList( "localhost:3333" ), 10L/*seconds*/ ) )
                .thenReturn(
                        getServers( singletonList( "localhost:5555" ), singletonList( "localhost:5555" ), singletonList( "localhost:5555" ) ) );

        RoutingDriver routingDriver = forSession( session, clock );

        // When
        routingDriver.session( AccessMode.WRITE );

        // Then
        assertThat( routingDriver.routingServers(), containsInAnyOrder( boltAddress( "localhost", 5555 ) ) );
        assertThat( routingDriver.readServers(), containsInAnyOrder( boltAddress( "localhost", 5555 ) ) );
        assertThat( routingDriver.writeServers(), containsInAnyOrder( boltAddress( "localhost", 5555 ) ) );
    }

    @Test
    public void shouldNotRediscoverWheNoTimeout()
    {
        // Given
        final Session session = mock( Session.class );
        Clock clock = mock( Clock.class );
        when(clock.millis()).thenReturn( 0L, 9900L, 18800L );
        when( session.run( GET_SERVERS ) )
                .thenReturn(
                        getServers( asList( "localhost:1111", "localhost:1112", "localhost:1113" ),
                                singletonList( "localhost:2222" ),
                                singletonList( "localhost:3333" ), 10L/*seconds*/ ) )
                .thenReturn(
                        getServers( singletonList( "localhost:5555" ), singletonList( "localhost:5555" ), singletonList( "localhost:5555" ) ) );

        RoutingDriver routingDriver = forSession( session, clock );

        // When
        routingDriver.session( AccessMode.WRITE );

        // Then
        assertThat( routingDriver.routingServers(), containsInAnyOrder( boltAddress( "localhost", 1111 ), boltAddress( "localhost", 1112 ), boltAddress( "localhost", 1113 ) ) );
        assertThat( routingDriver.readServers(), containsInAnyOrder( boltAddress( "localhost", 2222 ) ) );
        assertThat( routingDriver.writeServers(), containsInAnyOrder( boltAddress( "localhost", 3333 ) ) );
    }

    private RoutingDriver forSession( final Session session )
    {
        return forSession( session, Clock.SYSTEM );
    }
    private RoutingDriver forSession( final Session session, Clock clock )
    {
        return new RoutingDriver( SEED, pool, insecure(),
                new Function<Connection,Session>()
                {
                    @Override
                    public Session apply( Connection connection )
                    {
                        return session;
                    }
                }, clock, logging() );
    }

    private BoltServerAddress boltAddress( String host, int port )
    {
        return new BoltServerAddress( host, port );
    }


    StatementResult getServers( final List<String> routers, final List<String> readers,
            final List<String> writers )
    {
        return getServers( routers,readers, writers, Long.MAX_VALUE );
    }

    StatementResult getServers( final List<String> routers, final List<String> readers,
            final List<String> writers, final long ttl )
    {
        return new StatementResult()
        {
            private int counter = 0;

            @Override
            public List<String> keys()
            {
                return asList( "ttl", "servers" );
            }

            @Override
            public boolean hasNext()
            {
                return counter < 1;
            }

            @Override
            public Record next()
            {
                counter++;
                return new InternalRecord( asList( "ttl", "servers" ),
                        new Value[]{
                                value( ttl ),
                                value( asList( serverInfo( "ROUTE", routers ), serverInfo( "WRITE", writers ),
                                        serverInfo( "READ", readers ) ) )
                        } );
            }

            @Override
            public Record single() throws NoSuchRecordException
            {
                return next();
            }

            @Override
            public Record peek()
            {
                return null;
            }

            @Override
            public List<Record> list()
            {
                return null;
            }

            @Override
            public <T> List<T> list( Function<Record,T> mapFunction )
            {
                return null;
            }

            @Override
            public ResultSummary consume()
            {
                return null;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Map<String,Object> serverInfo( String role, List<String> addresses )
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "role", role );
        map.put( "addresses", addresses );

        return map;
    }

    private ConnectionPool pool()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        Connection connection = mock( Connection.class );
        when( connection.isOpen() ).thenReturn( true );
        when( pool.acquire( SEED ) ).thenReturn( connection );
        return pool;
    }

    private Logging logging()
    {
        Logging mock = mock( Logging.class );
        when( mock.getLog( anyString() ) ).thenReturn( mock( Logger.class ) );
        return mock;
    }
}