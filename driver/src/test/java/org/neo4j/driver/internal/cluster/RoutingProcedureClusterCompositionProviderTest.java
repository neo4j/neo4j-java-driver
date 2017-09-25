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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.Futures.getBlocking;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.v1.Values.value;

public class RoutingProcedureClusterCompositionProviderTest
{
    @Test
    public void shouldProtocolErrorWhenNoRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        RoutingProcedureResponse noRecordsResponse = newRoutingResponse();
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( noRecordsResponse ) );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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
    public void shouldProtocolErrorWhenMoreThanOneRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord, aRecord );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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
    public void shouldProtocolErrorWhenUnparsableRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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
    public void shouldProtocolErrorWhenNoRouters()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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
    public void shouldProtocolErrorWhenNoReaders()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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
    public void shouldPropagateConnectionFailureExceptions()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mock( Clock.class ),
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        doThrow( new ServiceUnavailableException( "Connection breaks during cypher execution" ) )
                .when( mockedRunner ).run( connectionStage );

        // When & Then
        try
        {
            provider.getClusterComposition( connectionStage );
            fail( "Expecting a failure but not triggered." );
        }
        catch( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), containsString( "Connection breaks during cypher execution" ) );
        }
    }

    @Test
    public void shouldReturnSuccessResultWhenNoError()
    {
        // Given
        Clock mockedClock = mock( Clock.class );
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        ClusterCompositionProvider provider = new RoutingProcedureClusterCompositionProvider( mockedClock,
                DEV_NULL_LOGGER, mockedRunner );

        CompletionStage<AsyncConnection> connectionStage = completedFuture( mock( AsyncConnection.class ) );
        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( connectionStage ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterCompositionResponse response = getBlocking( provider.getClusterComposition( connectionStage ) );

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

    private static RoutingProcedureRunner newProcedureRunnerMock()
    {
        return mock( RoutingProcedureRunner.class );
    }

    private static RoutingProcedureResponse newRoutingResponse( Record... records )
    {
        return new RoutingProcedureResponse( new Statement( "procedure" ), asList( records ) );
    }
}
