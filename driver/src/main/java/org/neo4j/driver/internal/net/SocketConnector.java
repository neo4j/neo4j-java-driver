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
package org.neo4j.driver.internal.net;

import java.util.Map;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

public class SocketConnector implements Connector
{
    private final ConnectionSettings connectionSettings;
    private final SecurityPlan securityPlan;
    private final Logging logging;

    public SocketConnector( ConnectionSettings connectionSettings, SecurityPlan securityPlan, Logging logging )
    {
        this.connectionSettings = connectionSettings;
        this.securityPlan = securityPlan;
        this.logging = logging;
    }

    @Override
    public final Connection connect( BoltServerAddress address )
    {
        Connection connection =
                createConnection( address, securityPlan, connectionSettings.connectTimeoutMillis(), logging );

        // Because SocketConnection is not thread safe, wrap it in this guard
        // to ensure concurrent access leads causes application errors
        connection = new ConcurrencyGuardingConnection( connection );

        try
        {
            connection.init( connectionSettings.userAgent(), tokenAsMap( connectionSettings.authToken() ) );
        }
        catch ( Throwable initError )
        {
            connection.close();
            throw initError;
        }

        return connection;
    }

    /**
     * Create new connection.
     * <p>
     * <b>This method is package-private only for testing</b>
     */
    Connection createConnection( BoltServerAddress address, SecurityPlan securityPlan, int timeoutMillis,
            Logging logging )
    {
        return new SocketConnection( address, securityPlan, timeoutMillis, logging );
    }

    private static Map<String,Value> tokenAsMap( AuthToken token )
    {
        if ( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException(
                    "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }
}
