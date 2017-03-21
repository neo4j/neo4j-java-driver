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
import java.util.Map;

import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

import static org.neo4j.driver.internal.SessionResourcesHandler.NO_OP;
import static org.neo4j.driver.internal.util.ServerVersion.v3_2_0;
import static org.neo4j.driver.internal.util.ServerVersion.version;
import static org.neo4j.driver.v1.Values.parameters;

public class GetServersProcedureRunner
{
    static final String GET_SERVERS = "dbms.cluster.routing.getServers";
    static final String GET_ROUTING_TABLE_PARAM = "context";
    static final String GET_ROUTING_TABLE = "dbms.cluster.routing.getRoutingTable({" + GET_ROUTING_TABLE_PARAM + "})";

    private final Map<String, String> routingContext;
    private Statement procedureCalled;

    public GetServersProcedureRunner( Map<String, String> context )
    {
        this.routingContext = context;
    }

    public List<Record> run( Connection connection )
    {
        if( version( connection.server().version() ).greaterThanOrEqual( v3_2_0 ) )
        {
            procedureCalled = new Statement( "CALL " + GET_ROUTING_TABLE,
                    parameters(GET_ROUTING_TABLE_PARAM, routingContext ) );
        }
        else
        {
            procedureCalled = new Statement("CALL " + GET_SERVERS );
        }

        return runProcedure( connection, procedureCalled );
    }

    List<Record> runProcedure( Connection connection, Statement procedure )
    {
        return NetworkSession.run( connection, procedure, NO_OP ).list();
    }

    Statement procedureCalled()
    {
        return procedureCalled;
    }
}
