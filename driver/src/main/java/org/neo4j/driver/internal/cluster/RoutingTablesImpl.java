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
import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.spi.ConnectionPool;

public class RoutingTablesImpl implements RoutingTables
{
    private final ConcurrentMap<String,RoutingTableHandler> routingTables; //TODO ever growing map?
    private final RoutingTableHandlerFactory factory;

    public RoutingTablesImpl( ConnectionPool connectionPool, RoutingTableFactory routingTableFactory, Rediscovery rediscovery, Logger log )
    {
        this( new ConcurrentHashMap<>(), new RoutingTableHandlerFactory( connectionPool, routingTableFactory, rediscovery, log ) );
    }

    RoutingTablesImpl( ConcurrentMap<String,RoutingTableHandler> routingTables, RoutingTableHandlerFactory factory )
    {
        this.factory = factory;
        this.routingTables = routingTables;
    }

    @Override
    public CompletionStage<RoutingTable> freshRoutingTable( String databaseName, AccessMode mode )
    {
        RoutingTableHandler handler = routingTableHandler( databaseName );
        return handler.freshRoutingTable( mode );
    }

    @Override
    public Set<BoltServerAddress> allServers()
    {
        Set<BoltServerAddress> servers = new HashSet<>();
        for ( RoutingTableHandler tableHandler : routingTables.values() )
        {
            servers.addAll( tableHandler.servers() );
        }
        return servers;
    }

    @Override
    public RoutingErrorHandler routingErrorHandler( String databaseName )
    {
        return routingTableHandler( databaseName );
    }

    private RoutingTableHandler routingTableHandler( String databaseName )
    {
        return routingTables.computeIfAbsent( databaseName, name -> factory.newInstance( name, this ) );
    }

    static class RoutingTableHandlerFactory
    {
        private final ConnectionPool connectionPool;
        private final RoutingTableFactory routingTableFactory;
        private final Rediscovery rediscovery;
        private final Logger log;

        RoutingTableHandlerFactory( ConnectionPool connectionPool, RoutingTableFactory routingTableFactory, Rediscovery rediscovery, Logger log )
        {
            this.connectionPool = connectionPool;
            this.routingTableFactory = routingTableFactory;
            this.rediscovery = rediscovery;
            this.log = log;
        }

        RoutingTableHandler newInstance( String databaseName, RoutingTables allTables )
        {
            return new RoutingTableHandler( routingTableFactory.newInstance( databaseName ), rediscovery, connectionPool, allTables, log );
        }
    }
}
