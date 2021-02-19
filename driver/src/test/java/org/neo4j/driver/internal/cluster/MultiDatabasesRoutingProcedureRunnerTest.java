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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.ReadOnlyBookmarkHolder;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.systemDatabase;
import static org.neo4j.driver.internal.InternalBookmark.empty;
import static org.neo4j.driver.internal.cluster.MultiDatabasesRoutingProcedureRunner.DATABASE_NAME;
import static org.neo4j.driver.internal.cluster.MultiDatabasesRoutingProcedureRunner.MULTI_DB_GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.RoutingProcedureRunner.ROUTING_CONTEXT;
import static org.neo4j.driver.util.TestUtil.await;

class MultiDatabasesRoutingProcedureRunnerTest extends AbstractRoutingProcedureRunnerTest
{
    @ParameterizedTest
    @ValueSource( strings = {"", SYSTEM_DATABASE_NAME, " this is a db name "} )
    void shouldCallGetRoutingTableWithEmptyMapOnSystemDatabaseForDatabase( String db )
    {
        TestRoutingProcedureRunner runner = new TestRoutingProcedureRunner( RoutingContext.EMPTY );
        RoutingProcedureResponse response = await( runner.run( connection(), database( db ), empty() ) );

        assertTrue( response.isSuccess() );
        assertEquals( 1, response.records().size() );

        assertThat( runner.bookmarkHolder, instanceOf( ReadOnlyBookmarkHolder.class ) );
        assertThat( runner.connection.databaseName(), equalTo( systemDatabase() ) );
        assertThat( runner.connection.mode(), equalTo( AccessMode.READ ) );

        Query query = generateMultiDatabaseRoutingQuery( EMPTY_MAP, db );
        assertThat( runner.procedure, equalTo(query) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", SYSTEM_DATABASE_NAME, " this is a db name "} )
    void shouldCallGetRoutingTableWithParamOnSystemDatabaseForDatabase( String db )
    {
        URI uri = URI.create( "neo4j://localhost/?key1=value1&key2=value2" );
        RoutingContext context = new RoutingContext( uri );

        TestRoutingProcedureRunner runner = new TestRoutingProcedureRunner( context );
        RoutingProcedureResponse response = await( runner.run( connection(), database( db ), empty() ) );

        assertTrue( response.isSuccess() );
        assertEquals( 1, response.records().size() );

        assertThat( runner.bookmarkHolder, instanceOf( ReadOnlyBookmarkHolder.class ) );
        assertThat( runner.connection.databaseName(), equalTo( systemDatabase() ) );
        assertThat( runner.connection.mode(), equalTo( AccessMode.READ ) );

        Query query = generateMultiDatabaseRoutingQuery( context.toMap(), db );
        assertThat( response.procedure(), equalTo(query) );
        assertThat( runner.procedure, equalTo(query) );
    }

    @Override
    RoutingProcedureRunner routingProcedureRunner( RoutingContext context )
    {
        return new TestRoutingProcedureRunner( context );
    }

    @Override
    RoutingProcedureRunner routingProcedureRunner( RoutingContext context, CompletionStage<List<Record>> runProcedureResult )
    {
        return new TestRoutingProcedureRunner( context, runProcedureResult );
    }

    private static Query generateMultiDatabaseRoutingQuery(Map context, String db )
    {
        Value parameters = parameters( ROUTING_CONTEXT, context, DATABASE_NAME, db );
        return new Query( MULTI_DB_GET_ROUTING_TABLE, parameters );
    }

    private static class TestRoutingProcedureRunner extends MultiDatabasesRoutingProcedureRunner
    {
        final CompletionStage<List<Record>> runProcedureResult;
        private Connection connection;
        private Query procedure;
        private BookmarkHolder bookmarkHolder;

        TestRoutingProcedureRunner( RoutingContext context )
        {
            this( context, completedFuture( singletonList( mock( Record.class ) ) ) );
        }

        TestRoutingProcedureRunner( RoutingContext context, CompletionStage<List<Record>> runProcedureResult )
        {
            super( context );
            this.runProcedureResult = runProcedureResult;
        }

        @Override
        CompletionStage<List<Record>> runProcedure(Connection connection, Query procedure, BookmarkHolder bookmarkHolder )
        {
            this.connection = connection;
            this.procedure = procedure;
            this.bookmarkHolder = bookmarkHolder;
            return runProcedureResult;
        }
    }
}
