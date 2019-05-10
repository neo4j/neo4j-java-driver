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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Logger;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;

public class RoutingTablesImpl implements RoutingTables
{
    private final ConcurrentMap<String,RoutingTableHandler> routingTables;
    private final RoutingTableHandlerFactory factory;
    private final Logger logger;

    public RoutingTablesImpl( ConnectionPool connectionPool, Rediscovery rediscovery, BoltServerAddress initialRouter, Clock clock, Logger logger )
    {
        this( new ConcurrentHashMap<>(), new RoutingTableHandlerFactory( connectionPool, rediscovery, initialRouter, clock, logger ), logger );
    }

    RoutingTablesImpl( ConcurrentMap<String,RoutingTableHandler> routingTables, RoutingTableHandlerFactory factory, Logger logger )
    {
        this.factory = factory;
        this.routingTables = routingTables;
        this.logger = logger;
    }

    @Override
    public CompletionStage<RoutingTableHandler> refreshRoutingTable( String databaseName, AccessMode mode )
    {
        RoutingTableHandler handler = getOrCreate( databaseName );
        return handler.refreshRoutingTable( mode ).thenApply( ignored -> handler );
    }

    @Override
    public Set<BoltServerAddress> allServers()
    {
        // obviously we just had a snapshot of all servers in all routing tables
        // after we read it, the set could already be changed.
        Set<BoltServerAddress> servers = new HashSet<>();
        for ( RoutingTableHandler tableHandler : routingTables.values() )
        {
            servers.addAll( tableHandler.servers() );
        }
        return servers;
    }

    @Override
    public void remove( String databaseName )
    {
        routingTables.remove( databaseName );
        logger.debug( "Routing table handler for database '%s' is removed.", databaseName );
    }

    @Override
    public void removeStale()
    {
        routingTables.forEach( ( databaseName, handler ) -> {
            if ( handler.isRoutingTableStale() )
            {
                logger.info( "Routing table handler for database '%s' is removed because it is not used for too long. Routing table: %s",
                        databaseName, handler.routingTable() );
                routingTables.remove( databaseName );
            }
        } );
    }

    // For tests
    public boolean contains( String database )
    {
        return routingTables.containsKey( database );
    }

    private RoutingTableHandler getOrCreate( String databaseName )
    {
        return routingTables.computeIfAbsent( databaseName, name -> {
            RoutingTableHandler handler = factory.newInstance( name, this );
            logger.debug( "Routing table handler for database '%s' is added.", databaseName );
            return handler;
        } );
    }

    static class RoutingTableHandlerFactory
    {
        private final ConnectionPool connectionPool;
        private final Rediscovery rediscovery;
        private final Logger log;
        private final BoltServerAddress initialRouter;
        private final Clock clock;

        RoutingTableHandlerFactory( ConnectionPool connectionPool, Rediscovery rediscovery, BoltServerAddress initialRouter, Clock clock, Logger log )
        {
            this.connectionPool = connectionPool;
            this.rediscovery = rediscovery;
            this.initialRouter = initialRouter;
            this.clock = clock;
            this.log = log;
        }

        RoutingTableHandler newInstance( String databaseName, RoutingTables allTables )
        {
            ClusterRoutingTable routingTable = new ClusterRoutingTable( databaseName, clock, initialRouter );
            return new RoutingTableHandler( routingTable, rediscovery, connectionPool, allTables, log );
        }
    }
}
