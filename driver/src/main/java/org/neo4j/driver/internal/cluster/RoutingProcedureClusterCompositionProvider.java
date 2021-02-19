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

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.value.ValueException;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;

import static java.lang.String.format;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.supportsMultiDatabase;

public class RoutingProcedureClusterCompositionProvider implements ClusterCompositionProvider
{
    private static final String PROTOCOL_ERROR_MESSAGE = "Failed to parse '%s' result received from server due to ";

    private final Clock clock;
    private final RoutingProcedureRunner routingProcedureRunner;
    private final RoutingProcedureRunner multiDatabaseRoutingProcedureRunner;

    public RoutingProcedureClusterCompositionProvider( Clock clock, RoutingContext routingContext )
    {
        this( clock, new RoutingProcedureRunner( routingContext ), new MultiDatabasesRoutingProcedureRunner( routingContext ) );
    }

    RoutingProcedureClusterCompositionProvider( Clock clock, RoutingProcedureRunner routingProcedureRunner,
            MultiDatabasesRoutingProcedureRunner multiDatabaseRoutingProcedureRunner )
    {
        this.clock = clock;
        this.routingProcedureRunner = routingProcedureRunner;
        this.multiDatabaseRoutingProcedureRunner = multiDatabaseRoutingProcedureRunner;
    }

    @Override
    public CompletionStage<ClusterComposition> getClusterComposition( Connection connection, DatabaseName databaseName, Bookmark bookmark )
    {
        RoutingProcedureRunner runner;
        if ( supportsMultiDatabase( connection ) )
        {
            runner = multiDatabaseRoutingProcedureRunner;
        }
        else
        {
            runner = routingProcedureRunner;
        }

        return runner.run( connection, databaseName, bookmark )
                .thenApply( this::processRoutingResponse );
    }

    private ClusterComposition processRoutingResponse( RoutingProcedureResponse response )
    {
        if ( !response.isSuccess() )
        {
            throw new CompletionException( format(
                    "Failed to run '%s' on server. Please make sure that there is a Neo4j server or cluster up running.",
                    invokedProcedureString( response ) ), response.error() );
        }

        List<Record> records = response.records();

        long now = clock.millis();

        // the record size is wrong
        if ( records.size() != 1 )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "records received '%s' is too few or too many.",
                    invokedProcedureString( response ), records.size() ) );
        }

        // failed to parse the record
        ClusterComposition cluster;
        try
        {
            cluster = ClusterComposition.parse( records.get( 0 ), now );
        }
        catch ( ValueException e )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "unparsable record received.",
                    invokedProcedureString( response ) ), e );
        }

        // the cluster result is not a legal reply
        if ( !cluster.hasRoutersAndReaders() )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "no router or reader found in response.",
                    invokedProcedureString( response ) ) );
        }

        // all good
        return cluster;
    }

    private static String invokedProcedureString( RoutingProcedureResponse response )
    {
        Query query = response.procedure();
        return query.text() + " " + query.parameters();
    }
}
