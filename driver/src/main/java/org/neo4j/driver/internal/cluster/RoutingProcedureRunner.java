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
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.ServerVersion;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

public class RoutingProcedureRunner
{
    static final String ROUTING_CONTEXT = "context";
    static final String GET_ROUTING_TABLE = "CALL dbms.cluster.routing.getRoutingTable($" + ROUTING_CONTEXT + ")";

    final RoutingContext context;

    public RoutingProcedureRunner( RoutingContext context )
    {
        this.context = context;
    }

    public CompletionStage<RoutingProcedureResponse> run( Connection connection, String databaseName, InternalBookmark bookmark )
    {
        DirectConnection delegate = connection( connection );
        Statement procedure = procedureStatement( connection.serverVersion(), databaseName );
        BookmarkHolder bookmarkHolder = bookmarkHolder( bookmark );
        return runProcedure( delegate, procedure, bookmarkHolder )
                .thenCompose( records -> releaseConnection( delegate, records ) )
                .handle( ( records, error ) -> processProcedureResponse( procedure, records, error ) );
    }

    DirectConnection connection( Connection connection )
    {
        return new DirectConnection( connection, ABSENT_DB_NAME, AccessMode.WRITE );
    }

    Statement procedureStatement( ServerVersion serverVersion, String databaseName )
    {
        if ( !Objects.equals( ABSENT_DB_NAME, databaseName ) )
        {
            throw new FatalDiscoveryException( String.format(
                    "Refreshing routing table for multi-databases is not supported in server version lower than 4.0. " +
                            "Current server version: %s. Database name: `%s`", serverVersion, databaseName ) );
        }
        return new Statement( GET_ROUTING_TABLE, parameters( ROUTING_CONTEXT, context.asMap() ) );
    }

    BookmarkHolder bookmarkHolder( InternalBookmark ignored )
    {
        return BookmarkHolder.NO_OP;
    }

    CompletionStage<List<Record>> runProcedure( Connection connection, Statement procedure, BookmarkHolder bookmarkHolder )
    {
        return connection.protocol()
                .runInAutoCommitTransaction( connection, procedure, bookmarkHolder, TransactionConfig.empty(), true )
                .asyncResult().thenCompose( StatementResultCursor::listAsync );
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

    private static RoutingProcedureResponse processProcedureResponse( Statement procedure, List<Record> records,
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

    private static RoutingProcedureResponse handleError( Statement procedure, Throwable error )
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
