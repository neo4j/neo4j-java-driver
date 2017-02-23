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

import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;

public class GetServersProcedureRunner
{
    private static final String CALL_GET_SERVERS = "CALL dbms.cluster.routing.getServers";

    public List<Record> run( Connection connection )
    {
        return NetworkSession.run( connection, new Statement( CALL_GET_SERVERS ), SessionResourcesHandler.NO_OP ).list();
    }
}
