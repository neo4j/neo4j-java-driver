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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import org.neo4j.driver.internal.EventHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.v1.Values.value;

public class ClusterCompositionProviderTest
{
    private final FakeClock clock = new FakeClock( (EventHandler) null, true );
    private final Connection connection = mock( Connection.class );

    @Test
    public void shouldParseClusterComposition() throws Exception
    {
        // given
        clock.progress( 16500 );
        keys( "ttl", "servers" );
        values( new Value[] {
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )} );

        // when
        ClusterComposition composition = getClusterComposition();

        // then
        assertNotNull( composition );
        assertEquals( 16500 + 100_000, composition.expirationTimestamp );
        assertEquals( serverSet( "one:1337", "two:1337" ), composition.readers() );
        assertEquals( serverSet( "one:1337" ), composition.writers() );
        assertEquals( serverSet( "one:1337", "two:1337" ), composition.routers() );
    }

    @Test
    public void shouldReturnNullIfResultContainsTooManyRows() throws Exception
    {
        // given
        keys( "ttl", "servers" );
        values(
                new Value[] {
                        value( 100 ), value( singletonList(
                        serverInfo( "READ", "one:1337", "two:1337" ) ) )},
                new Value[] {
                        value( 100 ), value( singletonList(
                        serverInfo( "WRITE", "one:1337" ) ) )},
                new Value[] {
                        value( 100 ), value( singletonList(
                        serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )} );

        // then
        assertNull( getClusterComposition() );
    }

    @Test
    public void shouldReturnNullOnEmptyResult() throws Exception
    {
        // given
        keys( "ttl", "servers" );
        values();

        // then
        assertNull( getClusterComposition() );
    }

    @Test
    public void shouldReturnNullOnResultWithWrongFormat() throws Exception
    {
        // given
        clock.progress( 16500 );
        keys( "ttl", "addresses" );
        values( new Value[] {
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )} );

        // then
        assertNull( getClusterComposition() );
    }

    @Test
    public void shouldPropagateConnectionFailureExceptions() throws Exception
    {
        // given
        ServiceUnavailableException expected = new ServiceUnavailableException( "spanish inquisition" );
        onGetServers( doThrow( expected ) );

        // when
        try
        {
            getClusterComposition();
            fail( "Expected exception" );
        }
        // then
        catch ( ServiceUnavailableException e )
        {
            assertSame( expected, e );
        }
    }

    private ClusterComposition getClusterComposition()
    {
        return new ClusterComposition.Provider.Default( clock ).getClusterComposition( connection );
    }

    private void keys( final String... keys )
    {
        onGetServers( doAnswer( withKeys( keys ) ) );
    }

    private void values( final Value[]... records )
    {
        onPullAll( doAnswer( withServerList( records ) ) );
    }

    private void onGetServers( Stubber stubber )
    {
        stubber.when( connection ).run(
                eq( ClusterComposition.Provider.GET_SERVERS ),
                eq( Collections.<String,Value>emptyMap() ),
                any( Collector.class ) );
    }

    private void onPullAll( Stubber stubber )
    {
        stubber.when( connection ).pullAll( any( Collector.class ) );
    }

    public static CollectorAnswer withKeys( final String... keys )
    {
        return new CollectorAnswer()
        {
            @Override
            void collect( Collector collector )
            {
                collector.keys( keys );
            }
        };
    }

    public static CollectorAnswer withServerList( final Value[]... records )
    {
        return new CollectorAnswer()
        {
            @Override
            void collect( Collector collector )
            {
                for ( Value[] fields : records )
                {
                    collector.record( fields );
                }
            }
        };
    }

    private static abstract class CollectorAnswer implements Answer
    {
        abstract void collect( Collector collector );

        @Override
        public final Object answer( InvocationOnMock invocation ) throws Throwable
        {
            Collector collector = collector( invocation );
            collect( collector );
            collector.done();
            return null;
        }

        private Collector collector( InvocationOnMock invocation )
        {
            switch ( invocation.getMethod().getName() )
            {
            case "pullAll":
                return invocation.getArgumentAt( 0, Collector.class );
            case "run":
                return invocation.getArgumentAt( 2, Collector.class );
            default:
                throw new UnsupportedOperationException( invocation.getMethod().getName() );
            }
        }
    }

    public static Map<String,Object> serverInfo( String role, String... addresses )
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "role", role );
        map.put( "addresses", asList( addresses ) );
        return map;
    }

    private static Set<BoltServerAddress> serverSet( String... addresses )
    {
        Set<BoltServerAddress> result = new HashSet<>();
        for ( String address : addresses )
        {
            result.add( new BoltServerAddress( address ) );
        }
        return result;
    }
}
