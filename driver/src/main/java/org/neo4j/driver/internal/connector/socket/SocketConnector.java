/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.connector.socket;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.Version;
import org.neo4j.driver.internal.connector.ConcurrencyGuardingConnection;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.BoltServerAddress;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;

public class SocketConnector implements Connector
{
    public static final String SCHEME = "bolt";

    @Override
    public boolean supports( String scheme )
    {
        return scheme.equals( SCHEME );
    }

    @Override
    public Connection connect( BoltServerAddress address, SecurityPlan securityPlan, Logging logging ) throws ClientException
    {
        Connection conn = new SocketConnection( address, securityPlan, logging );

        // Because SocketConnection is not thread safe, wrap it in this guard
        // to ensure concurrent access leads causes application errors
        conn = new ConcurrencyGuardingConnection( conn );
        conn.init( "bolt-java-driver/" + Version.driverVersion(), tokenAsMap( securityPlan.authToken() ) );
        return conn;
    }

    @Override
    public Collection<String> supportedSchemes()
    {
        return Collections.singletonList( SCHEME );
    }

    private Map<String,Value> tokenAsMap( AuthToken token )
    {
        if( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException( "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                                       "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }
}
