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

import java.util.List;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.value.ValueException;

import static java.lang.String.format;

public class GetServersProcedureClusterCompositionProvider implements ClusterCompositionProvider
{

    private final String PROTOCOL_ERROR_MESSAGE = "Failed to parse `%s' result received from server due to ";

    private final Clock clock;
    private final Logger log;
    private final GetServersProcedureRunner getServersRunner;

    public GetServersProcedureClusterCompositionProvider( Clock clock, Logger log, RoutingSettings settings )
    {
        this( clock, log, new GetServersProcedureRunner( settings.routingParameters ) );
    }

    GetServersProcedureClusterCompositionProvider( Clock clock, Logger log, GetServersProcedureRunner getServersRunner )
    {
        this.clock = clock;
        this.log = log;
        this.getServersRunner = getServersRunner;
    }

    @Override
    public ClusterCompositionResponse getClusterComposition( Connection connection )
    {
        List<Record> records;

        // failed to invoke procedure
        try
        {
            records = getServersRunner.run( connection );
        }
        catch ( ClientException e )
        {
            return new ClusterCompositionResponse.Failure( new ServiceUnavailableException( format(
                    "Failed to run '%s' on server. " +
                    "Please make sure that there is a Neo4j 3.1+ causal cluster up running.",
                    getServersRunner.procedureCalled() ), e
            ) );
        }

        log.info( "Got getServers response: %s", records );
        long now = clock.millis();

        // the record size is wrong
        if ( records.size() != 1 )
        {
            return new ClusterCompositionResponse.Failure( new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE +
                    "records received '%s' is too few or too many.", getServersRunner.procedureCalled(),
                    records.size() ) ) );
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
                    PROTOCOL_ERROR_MESSAGE +
                    "unparsable record received.", getServersRunner.procedureCalled() ), e ) );
        }

        // the cluster result is not a legal reply
        if ( !cluster.hasRoutersAndReaders() )
        {
            return new ClusterCompositionResponse.Failure( new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE +
                    "no router or reader found in response.", getServersRunner.procedureCalled() ) ) );
        }

        // all good
        return new ClusterCompositionResponse.Success( cluster );
    }
}
