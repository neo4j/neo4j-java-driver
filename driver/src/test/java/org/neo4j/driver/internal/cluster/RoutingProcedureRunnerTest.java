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

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.GET_ROUTING_TABLE_PARAM;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.GET_SERVERS;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.ServerVersion.version;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class RoutingProcedureRunnerTest
{
    @Test
    public void shouldCallGetRoutingTableWithEmptyMap()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( asList( mock( Record.class ), mock( Record.class ) ) ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.1" ) ) );

        assertTrue( response.isSuccess() );
        assertEquals( 2, response.records().size() );
        assertEquals( new Statement( "CALL " + GET_ROUTING_TABLE, parameters( GET_ROUTING_TABLE_PARAM, EMPTY_MAP ) ),
                response.procedure() );
    }

    @Test
    public void shouldCallGetRoutingTableWithParam()
    {
        URI uri = URI.create( "bolt+routing://localhost/?key1=value1&key2=value2" );
        RoutingContext context = new RoutingContext( uri );

        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( context,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.1" ) ) );

        assertTrue( response.isSuccess() );
        assertEquals( 1, response.records().size() );
        Value expectedParams = parameters( GET_ROUTING_TABLE_PARAM, context.asMap() );
        assertEquals( new Statement( "CALL " + GET_ROUTING_TABLE, expectedParams ), response.procedure() );
    }

    @Test
    public void shouldCallGetServers()
    {
        URI uri = URI.create( "bolt+routing://localhost/?key1=value1&key2=value2" );
        RoutingContext context = new RoutingContext( uri );

        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( context,
                completedFuture( asList( mock( Record.class ), mock( Record.class ) ) ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.1.8" ) ) );

        assertTrue( response.isSuccess() );
        assertEquals( 2, response.records().size() );
        assertEquals( new Statement( "CALL " + GET_SERVERS ), response.procedure() );
    }

    @Test
    public void shouldReturnFailedResponseOnClientException()
    {
        ClientException error = new ClientException( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY, failedFuture( error ) );

        RoutingProcedureResponse response = await( runner.run( connectionStage( "Neo4j/3.2.2" ) ) );

        assertFalse( response.isSuccess() );
        assertEquals( error, response.error() );
    }

    @Test
    public void shouldReturnFailedStageOnError()
    {
        Exception error = new Exception( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY, failedFuture( error ) );

        try
        {
            await( runner.run( connectionStage( "Neo4j/3.2.2" ) ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldPropagateErrorFromConnectionStage()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY );

        try
        {
            await( runner.run( failedFuture( error ) ) );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldReleaseConnectionOnSuccess()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        CompletionStage<Connection> connectionStage = connectionStage( "Neo4j/3.2.2" );
        Connection connection = await( connectionStage );
        RoutingProcedureResponse response = await( runner.run( connectionStage ) );

        assertTrue( response.isSuccess() );
        verify( connection ).release();
    }

    @Test
    public void shouldPropagateReleaseError()
    {
        RoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY,
                completedFuture( singletonList( mock( Record.class ) ) ) );

        RuntimeException releaseError = new RuntimeException( "Release failed" );
        CompletionStage<Connection> connectionStage = connectionStage( "Neo4j/3.3.3", failedFuture( releaseError ) );
        Connection connection = await( connectionStage );

        try
        {
            await( runner.run( connectionStage ) );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( releaseError, e );
        }
        verify( connection ).release();
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

        TestRoutingProcedureRunner( RoutingContext context )
        {
            this( context, null );
        }

        TestRoutingProcedureRunner( RoutingContext context, CompletionStage<List<Record>> runProcedureResult )
        {
            super( context );
            this.runProcedureResult = runProcedureResult;
        }

        @Override
        CompletionStage<List<Record>> runProcedure( Connection connection, Statement procedure )
        {
            return runProcedureResult;
        }
    }

}
