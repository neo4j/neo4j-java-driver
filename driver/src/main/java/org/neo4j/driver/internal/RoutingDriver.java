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
package org.neo4j.driver.internal;

import org.neo4j.driver.internal.cluster.LoadBalancer;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.String.format;

public class RoutingDriver extends BaseDriver
{
    // Verify that a security plan is compatible with this driver, throwing an exception if not
    private static SecurityPlan verifiedSecurityPlan( SecurityPlan securityPlan )
    {
        if ( !securityPlan.isRoutingCompatible() )
        {
            throw new IllegalArgumentException( "The chosen security plan is not compatible with a routing driver" );
        }
        return securityPlan;
    }

    private final LoadBalancer loadBalancer;

    public RoutingDriver(
            RoutingSettings settings,
            BoltServerAddress seedAddress,
            ConnectionPool connections,
            SecurityPlan securityPlan,
            Clock clock,
            Logging logging )
    {
        super( verifiedSecurityPlan( securityPlan ), logging );
        this.loadBalancer = new LoadBalancer( settings, clock, log, connections, seedAddress );
    }

    @Override
    protected Session newSessionWithMode( AccessMode mode )
    {
        Connection connection = acquireConnection( mode );
        NetworkSession networkSession = new NetworkSession( connection );
        return new RoutingNetworkSession( networkSession, mode, connection.boltServerAddress(), loadBalancer );
    }

    private Connection acquireConnection( AccessMode role )
    {
        switch ( role )
        {
        case READ:
            return loadBalancer.acquireReadConnection();
        case WRITE:
            return loadBalancer.acquireWriteConnection();
        default:
            throw new ClientException( role + " is not supported for creating new sessions" );
        }
    }

    @Override
    protected void closeResources()
    {
        try
        {
            loadBalancer.close();
        }
        catch ( Exception ex )
        {
            log.error( format( "~~ [ERROR] %s", ex.getMessage() ), ex );
        }
    }
}
