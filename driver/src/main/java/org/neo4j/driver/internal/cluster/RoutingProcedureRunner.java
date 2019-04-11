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

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.ServerVersion;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;
import static org.neo4j.driver.internal.util.ServerVersion.v3_2_0;
import static org.neo4j.driver.internal.util.ServerVersion.v4_0_0;

public class RoutingProcedureRunner
{
    static final String GET_SERVERS = "dbms.cluster.routing.getServers";

    static final String ROUTING_CONTEXT = "context";
    static final String GET_ROUTING_TABLE = "dbms.cluster.routing.getRoutingTable({" + ROUTING_CONTEXT + "})";
    static final String DATABASE_NAME = "database";
    static final String MULTI_DB_GET_ROUTING_TABLE = String.format( "dbms.cluster.routing.getRoutingTable({%s}, %s)", ROUTING_CONTEXT, DATABASE_NAME );

    private final RoutingContext context;

    public RoutingProcedureRunner( RoutingContext context )
    {
        this.context = context;
    }

    public CompletionStage<RoutingProcedureResponse> run( CompletionStage<Connection> connectionStage, String databaseName )
    {
        return connectionStage.thenCompose( connection ->
        {
            ServerVersion serverVersion = connection.serverVersion();
            DirectConnection delegate = new DirectConnection( connection, selectDatabase( serverVersion ), AccessMode.WRITE );
            Statement procedure = procedureStatement( serverVersion, databaseName );
            return runProcedure( delegate, procedure )
                    .thenCompose( records -> releaseConnection( delegate, records ) )
                    .handle( ( records, error ) -> processProcedureResponse( procedure, records, error ) );
        } );
    }

    CompletionStage<List<Record>> runProcedure( Connection connection, Statement procedure )
    {
        return connection.protocol() // this line fails if database name is provided in BOLT versions that do not support database name.
                .runInAutoCommitTransaction( connection, procedure, BookmarksHolder.NO_OP, TransactionConfig.empty(), true )
                .asyncResult().thenCompose( StatementResultCursor::listAsync );
    }

    private Statement procedureStatement( ServerVersion serverVersion, String databaseName )
    {
        /*
         * For v4.0+ databases, call procedure to get routing table for the database specified.
         * For database version lower than 4.0, the database name will be ignored.
         */
        if ( serverVersion.greaterThanOrEqual( v4_0_0 ) )
        {
            return new Statement( "CALL " + MULTI_DB_GET_ROUTING_TABLE,
                    parameters( ROUTING_CONTEXT, context.asMap(), DATABASE_NAME, databaseName ) );
        }
        else if ( serverVersion.greaterThanOrEqual( v3_2_0 ) )
        {
            return new Statement( "CALL " + GET_ROUTING_TABLE,
                    parameters( ROUTING_CONTEXT, context.asMap() ) );
        }
        else
        {
            return new Statement( "CALL " + GET_SERVERS );
        }
    }

    private String selectDatabase( ServerVersion serverVersion )
    {
        if ( serverVersion.greaterThanOrEqual( v4_0_0 ) )
        {
            // Routing procedure will be called on the system database
            return SYSTEM_DB_NAME;
        }
        else
        {
            // For lower bolt versions, there is no system database, so we should just run on the "default" database
            return ABSENT_DB_NAME;
        }
    }

    private CompletionStage<List<Record>> releaseConnection( Connection connection, List<Record> records )
    {
        // It is not strictly required to release connection after routing procedure invocation because it'll
        // be released by the PULL_ALL response handler after result is fully fetched. Such release will happen
        // in background. However, releasing it early as part of whole chain makes it easier to reason about
        // rediscovery in stub server tests. Some of them assume connections to instances not present in new
        // routing table will be closed immediately.
        return connection.release().thenApply( ignore -> records );
    }

    private RoutingProcedureResponse processProcedureResponse( Statement procedure, List<Record> records,
            Throwable error )
    {
        Throwable cause = Futures.completionExceptionCause( error );
        if ( cause != null )
        {
            return handleError( procedure, cause );
        }
        else
        {
            return new RoutingProcedureResponse( procedure, records );
        }
    }

    private RoutingProcedureResponse handleError( Statement procedure, Throwable error )
    {
        if ( error instanceof ClientException )
        {
            return new RoutingProcedureResponse( procedure, error );
        }
        else
        {
            throw new CompletionException( error );
        }
    }
}
