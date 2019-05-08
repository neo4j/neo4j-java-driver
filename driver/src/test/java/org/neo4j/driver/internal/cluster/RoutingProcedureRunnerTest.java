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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.DATABASE_NAME;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.MULTI_DB_GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.ROUTING_CONTEXT;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.ServerVersion.version;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingProcedureRunnerTest
{
    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, " this is a db name "} )
    void shouldCallGetRoutingTableWithEmptyMapOnSystemDatabaseForDatabase( String db )
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( asList( mock( Record.class ), mock( Record.class ) ) ), SYSTEM_DB_NAME );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/4.0.0" ), db ) );

        assertTrue( response.isSuccess() );
        assertEquals( 2, response.records().size() );

        Value parameters = generateMultiDatabaseRoutingParameters( EMPTY_MAP, db );
        assertEquals( new Statement( "CALL " + MULTI_DB_GET_ROUTING_TABLE, parameters ), response.procedure() );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, " this is a db name "} )
    void shouldCallGetRoutingTableWithParamOnSystemDatabaseForDatabase( String db )
    {
        URI uri = URI.create( "neo4j://localhost/?key1=value1&key2=value2" );
        RoutingContext context = new RoutingContext( uri );

        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( context,
                completedFuture( singletonList( mock( Record.class ) ) ), SYSTEM_DB_NAME );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/4.0.0" ), db ) );

        assertTrue( response.isSuccess() );
        assertEquals( 1, response.records().size() );
        Value expectedParams = generateMultiDatabaseRoutingParameters( context.asMap(), db );
        assertEquals( new Statement( "CALL " + MULTI_DB_GET_ROUTING_TABLE, expectedParams ), response.procedure() );
    }

    @Test
    void shouldCallGetRoutingTableWithEmptyMap()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( asList( mock( Record.class ), mock( Record.class ) ) ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.1" ), ABSENT_DB_NAME ) );

        assertTrue( response.isSuccess() );
        assertEquals( 2, response.records().size() );
        assertEquals( new Statement( "CALL " + GET_ROUTING_TABLE, parameters( ROUTING_CONTEXT, EMPTY_MAP ) ),
                response.procedure() );
    }

    @Test
    void shouldCallGetRoutingTableWithParam()
    {
        URI uri = URI.create( "neo4j://localhost/?key1=value1&key2=value2" );
        RoutingContext context = new RoutingContext( uri );

        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( context,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.1" ), ABSENT_DB_NAME ) );

        assertTrue( response.isSuccess() );
        assertEquals( 1, response.records().size() );
        Value expectedParams = parameters( ROUTING_CONTEXT, context.asMap() );
        assertEquals( new Statement( "CALL " + GET_ROUTING_TABLE, expectedParams ), response.procedure() );
    }

    @Test
    void shouldReturnFailedResponseOnClientException()
    {
        ClientException error = new ClientException( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY, failedFuture( error ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.2" ), ABSENT_DB_NAME ) );

        assertFalse( response.isSuccess() );
        assertEquals( error, response.error() );
    }

    @Test
    void shouldReturnFailedStageOnError()
    {
        Exception error = new Exception( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY, failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> await( runner.run( connectionStage( "Neo4j/3.2.2" ), ABSENT_DB_NAME ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateErrorFromConnectionStage()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( runner.run( failedFuture( error ), ABSENT_DB_NAME ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldReleaseConnectionOnSuccess()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        CompletionStage<Connection> connectionStage = connectionStage( "Neo4j/3.2.2" );
        Connection connection = await( connectionStage );
        RoutingProcedureResponse response = await( runner.run( connectionStage, ABSENT_DB_NAME ) );

        assertTrue( response.isSuccess() );
        verify( connection ).release();
    }

    @Test
    void shouldPropagateReleaseError()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        RuntimeException releaseError = new RuntimeException( "Release failed" );
        CompletionStage<Connection> connectionStage = connectionStage( "Neo4j/3.3.3", failedFuture( releaseError ) );
        Connection connection = await( connectionStage );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( runner.run( connectionStage, ABSENT_DB_NAME ) ) );
        assertEquals( releaseError, e );
        verify( connection ).release();
    }

    private static Value generateMultiDatabaseRoutingParameters( Map context, String db )
    {
        if ( Objects.equals( ABSENT_DB_NAME, db ) )
        {
            db = null;
        }
        return parameters( ROUTING_CONTEXT, context, DATABASE_NAME, db );
    }

    private static CompletionStage<Connection> connectionStage( String serverVersion )
    {
        return connectionStage( serverVersion, completedWithNull() );
    }

    private static CompletionStage<Connection> connectionStage( String serverVersion,
            CompletionStage<Void> releaseStage )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( new BoltServerAddress( "123:45" ) );
        when( connection.serverVersion() ).thenReturn( version( serverVersion ) );
        when( connection.release() ).thenReturn( releaseStage );
        return completedFuture( connection );
    }

    private static class TestRoutingProcedureRunner extends RoutingProcedureRunner
    {
        final CompletionStage<List<Record>> runProcedureResult;
        final String executionDatabase;

        TestRoutingProcedureRunner( RoutingContext context )
        {
            this( context, null, ABSENT_DB_NAME );
        }

        TestRoutingProcedureRunner( RoutingContext context, CompletionStage<List<Record>> runProcedureResult )
        {
            this( context, runProcedureResult, ABSENT_DB_NAME );
        }

        TestRoutingProcedureRunner( RoutingContext context, CompletionStage<List<Record>> runProcedureResult, String executionDatabase )
        {
            super( context );
            this.runProcedureResult = runProcedureResult;
            this.executionDatabase = executionDatabase;
        }

        @Override
        CompletionStage<List<Record>> runProcedure( Connection connection, Statement procedure )
        {
            assertEquals( executionDatabase, connection.databaseName() );
            return runProcedureResult;
        }
    }

}
