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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.value.StringValue;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.InternalBookmark.empty;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingProcedureClusterCompositionProviderTest
{
    @Test
    void shouldProtocolErrorWhenNoRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection );

        RoutingProcedureResponse noRecordsResponse = newRoutingResponse();
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any( InternalBookmark.class ) ) )
                .thenReturn( completedFuture( noRecordsResponse ) );

        // When & Then
        ProtocolException error = assertThrows( ProtocolException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( error.getMessage(), containsString( "records received '0' is too few or too many." ) );
    }

    @Test
    void shouldProtocolErrorWhenMoreThanOneRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection );

        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord, aRecord );
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ProtocolException error = assertThrows( ProtocolException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( error.getMessage(), containsString( "records received '2' is too few or too many." ) );
    }

    @Test
    void shouldProtocolErrorWhenUnparsableRecord()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection );

        Record aRecord = new InternalRecord( asList( "key1", "key2" ), new Value[]{ new StringValue( "a value" ) } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( aRecord );
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedFuture( routingResponse ) );

        // When
        ProtocolException error = assertThrows( ProtocolException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( error.getMessage(), containsString( "unparsable record received." ) );
    }

    @Test
    void shouldProtocolErrorWhenNoRouters()
    {
        // Given
        MultiDatabasesRoutingProcedureRunner mockedRunner = newMultiDBProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection, mockedClock );

        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ProtocolException error = assertThrows( ProtocolException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( error.getMessage(), containsString( "no router or reader found in response." ) );
    }

    @Test
    void shouldProtocolErrorWhenNoReaders()
    {
        // Given
        MultiDatabasesRoutingProcedureRunner mockedRunner = newMultiDBProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        Clock mockedClock = mock( Clock.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection, mockedClock );

        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ProtocolException error = assertThrows( ProtocolException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( error.getMessage(), containsString( "no router or reader found in response." ) );
    }

    @Test
    void shouldPropagateConnectionFailureExceptions()
    {
        // Given
        RoutingProcedureRunner mockedRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection );

        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( failedFuture(
                new ServiceUnavailableException( "Connection breaks during cypher execution" ) ) );

        // When & Then
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertThat( e.getMessage(), containsString( "Connection breaks during cypher execution" ) );
    }

    @Test
    void shouldReturnSuccessResultWhenNoError()
    {
        // Given
        Clock mockedClock = mock( Clock.class );
        Connection connection = mock( Connection.class );
        MultiDatabasesRoutingProcedureRunner mockedRunner = newMultiDBProcedureRunnerMock();
        ClusterCompositionProvider provider =
                newClusterCompositionProvider( mockedRunner, connection, mockedClock );

        Record record = new InternalRecord( asList( "ttl", "servers" ), new Value[]{
                value( 100 ), value( asList(
                serverInfo( "READ", "one:1337", "two:1337" ),
                serverInfo( "WRITE", "one:1337" ),
                serverInfo( "ROUTE", "one:1337", "two:1337" ) ) )
        } );
        RoutingProcedureResponse routingResponse = newRoutingResponse( record );
        when( mockedRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) )
                .thenReturn( completedFuture( routingResponse ) );
        when( mockedClock.millis() ).thenReturn( 12345L );

        // When
        ClusterComposition cluster = await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) );

        // Then
        assertEquals( 12345 + 100_000, cluster.expirationTimestamp() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.readers() );
        assertEquals( serverSet( "one:1337" ), cluster.writers() );
        assertEquals( serverSet( "one:1337", "two:1337" ), cluster.routers() );
    }

    @Test
    void shouldReturnFailureWhenProcedureRunnerFails()
    {
        RoutingProcedureRunner procedureRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );

        RuntimeException error = new RuntimeException( "hi" );
        when( procedureRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) )
                .thenReturn( completedFuture( newRoutingResponse( error ) ) );

        RoutingProcedureClusterCompositionProvider provider =
                newClusterCompositionProvider( procedureRunner, connection );

        RuntimeException e = assertThrows( RuntimeException.class,
                () -> await( provider.getClusterComposition( connection, defaultDatabase(), empty() ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldUseMultiDBProcedureRunnerWhenConnectingWith40Server() throws Throwable
    {
        MultiDatabasesRoutingProcedureRunner procedureRunner = newMultiDBProcedureRunnerMock();
        Connection connection = mock( Connection.class );

        RoutingProcedureClusterCompositionProvider provider =
                newClusterCompositionProvider( procedureRunner, connection );

        when( procedureRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedWithNull() );
        provider.getClusterComposition( connection, defaultDatabase(), empty() );

        verify( procedureRunner ).run( eq( connection ), any( DatabaseName.class ), any( InternalBookmark.class ) );
    }

    @Test
    void shouldUseProcedureRunnerWhenConnectingWith35AndPreviousServers() throws Throwable
    {
        RoutingProcedureRunner procedureRunner = newProcedureRunnerMock();
        Connection connection = mock( Connection.class );

        RoutingProcedureClusterCompositionProvider provider =
                newClusterCompositionProvider( procedureRunner, connection );

        when( procedureRunner.run( eq( connection ), any( DatabaseName.class ), any(InternalBookmark.class ) ) ).thenReturn( completedWithNull() );
        provider.getClusterComposition( connection, defaultDatabase(), empty() );

        verify( procedureRunner ).run( eq( connection ), any( DatabaseName.class ), any( InternalBookmark.class ) );
    }

    private static Map<String,Object> serverInfo( String role, String... addresses )
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

    private static MultiDatabasesRoutingProcedureRunner newMultiDBProcedureRunnerMock()
    {
        return mock( MultiDatabasesRoutingProcedureRunner.class );
    }

    private static RoutingProcedureResponse newRoutingResponse( Record... records )
    {
        return new RoutingProcedureResponse( new Query( "procedure" ), asList( records ) );
    }

    private static RoutingProcedureResponse newRoutingResponse( Throwable error )
    {
        return new RoutingProcedureResponse( new Query( "procedure" ), error );
    }
    
    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider( RoutingProcedureRunner runner, Connection connection )
    {
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_5_0 );
        when( connection.protocol() ).thenReturn( BoltProtocolV3.INSTANCE );
        return new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), runner, newMultiDBProcedureRunnerMock() );
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider( MultiDatabasesRoutingProcedureRunner runner, Connection connection )
    {
        when( connection.serverVersion() ).thenReturn( ServerVersion.v4_0_0 );
        when( connection.protocol() ).thenReturn( BoltProtocolV4.INSTANCE );
        return new RoutingProcedureClusterCompositionProvider( mock( Clock.class ), newProcedureRunnerMock(), runner );
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider( MultiDatabasesRoutingProcedureRunner runner, Connection connection, Clock clock )
    {
        when( connection.serverVersion() ).thenReturn( ServerVersion.v4_0_0 );
        when( connection.protocol() ).thenReturn( BoltProtocolV4.INSTANCE );
        return new RoutingProcedureClusterCompositionProvider( clock, newProcedureRunnerMock(), runner );
    }
}
