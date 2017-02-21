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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.v1.Values.value;

public class ClusterCompositionProviderTest
{
    @Test
    public void shouldProtocolErrorWhenNoRecord() throws Throwable
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        ArrayList<Record> emptyRecord = new ArrayList<>();
        when( mockedRunner.run( mockedConn ) ).thenReturn( emptyRecord );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        try
        {
            response.clusterComposition();
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ProtocolException.class ) );
            assertThat( e.getMessage(), containsString( "records received '0' is too few or too many." ) );
        }
    }

    @Test
    public void shouldProtocolErrorWhenMoreThanOneRecord() throws Throwable
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        when( mockedRunner.run( mockedConn ) ).thenReturn( asList( aRecord, aRecord ) );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        try
        {
            response.clusterComposition();
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ProtocolException.class ) );
            assertThat( e.getMessage(), containsString( "records received '2' is too few or too many." ) );
        }
    }

    @Test
    public void shouldProtocolErrorWhenUnparsableRecord() throws Throwable
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        when( mockedRunner.run( mockedConn ) ).thenReturn( asList( aRecord ) );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        try
        {
            response.clusterComposition();
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ProtocolException.class ) );
            assertThat( e.getMessage(), containsString( "unparsable record received." ) );
        }
    }

    @Test
    public void shouldProtocolErrorWhenNoRouters() throws Throwable
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ) ) )
        } );
        when( mockedRunner.run( mockedConn ) ).thenReturn( asList( record ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        try
        {
            response.clusterComposition();
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ProtocolException.class ) );
            assertThat( e.getMessage(), containsString( "no router or reader found in response." ) );
        }
    }

    @Test
    public void shouldProtocolErrorWhenNoReaders() throws Throwable
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        when( mockedRunner.run( mockedConn ) ).thenReturn( asList( record ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Failure.class ) );
        try
        {
            response.clusterComposition();
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ProtocolException.class ) );
            assertThat( e.getMessage(), containsString( "no router or reader found in response." ) );
        }
    }


    @Test
    public void shouldPropagateConnectionFailureExceptions() throws Exception
    {
        // Given
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        doThrow( new ServiceUnavailableException( "Connection breaks during cypher execution" ) )
                .when( mockedConn ).run( any( String.class ), anyMap(), any( Collector.class ) );

        // When & Then
        try
        {
            provider.getClusterComposition( mockedConn );
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), containsString( "Connection breaks during cypher execution" ) );
        }
    }

    @Test
    public void shouldReturnSuccessResultWhenNoError() throws Throwable
    {
        // Given
        Clock mockedClock = mock( Clock.class );
        GetServersProcedureRunner mockedRunner = mock( GetServersProcedureRunner.class );
        ClusterCompositionProvider provider = new GetServersProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        PooledConnection mockedConn = mock( PooledConnection.class );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        when( mockedRunner.run( mockedConn ) ).thenReturn( asList( record ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = provider.getClusterComposition( mockedConn );

        // Then
        assertThat( response, instanceOf( ClusterCompositionResponse.Success.class ) );
        ClusterComposition cluster = response.clusterComposition();
        assertEquals( 12345 + 100_000, cluster.expirationTimestamp() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.readers() );
        assertEquals( serverSet( "one:1337" ), cluster.writers() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.routers() );
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
