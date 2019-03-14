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
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.value.ValueException;

import static java.lang.String.format;

public class RoutingProcedureClusterCompositionProvider implements ClusterCompositionProvider
{
    private static final String PROTOCOL_ERROR_MESSAGE = "Failed to parse '%s' result received from server due to ";

    private final Clock clock;
    private final RoutingProcedureRunner routingProcedureRunner;

    public RoutingProcedureClusterCompositionProvider( Clock clock, RoutingSettings settings )
    {
        this( clock, new RoutingProcedureRunner( settings.routingContext() ) );
    }

    RoutingProcedureClusterCompositionProvider( Clock clock, RoutingProcedureRunner routingProcedureRunner )
    {
        this.clock = clock;
        this.routingProcedureRunner = routingProcedureRunner;
    }

    @Override
    public CompletionStage<ClusterCompositionResponse> getClusterComposition(
            CompletionStage<Connection> connectionStage )
    {
        return routingProcedureRunner.run( connectionStage )
                .thenApply( this::processRoutingResponse );
    }

    private ClusterCompositionResponse processRoutingResponse( RoutingProcedureResponse response )
    {
        if ( !response.isSuccess() )
        {
            return new ClusterCompositionResponse.Failure( new ServiceUnavailableException( format(
                    "Failed to run '%s' on server. " +
                    "Please make sure that there is a Neo4j 3.1+ causal cluster up running.",
                    invokedProcedureString( response ) ), response.error()
            ) );
        }

        List<Record> records = response.records();

        long now = clock.millis();

        // the record size is wrong
        if ( records.size() != 1 )
        {
            return new ClusterCompositionResponse.Failure( new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "records received '%s' is too few or too many.",
                    invokedProcedureString( response ), records.size() ) ) );
        }

        // failed to parse the record
        ClusterComposition cluster;
        try
        {
            cluster = ClusterComposition.parse( records.get( 0 ), now );
        }
        catch ( ValueException e )
        {
            return new ClusterCompositionResponse.Failure( new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "unparsable record received.",
                    invokedProcedureString( response ) ), e ) );
        }

        // the cluster result is not a legal reply
        if ( !cluster.hasRoutersAndReaders() )
        {
            return new ClusterCompositionResponse.Failure( new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "no router or reader found in response.",
                    invokedProcedureString( response ) ) ) );
        }

        // all good
        return new ClusterCompositionResponse.Success( cluster );
    }

    private static String invokedProcedureString( RoutingProcedureResponse response )
    {
        Statement statement = response.procedure();
        return statement.text() + " " + statement.parameters();
    }
}
