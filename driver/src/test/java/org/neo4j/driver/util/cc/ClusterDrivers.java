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
package org.neo4j.driver.util.cc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.util.cc.ClusterMemberRoleDiscoveryFactory.ClusterMemberRoleDiscovery;

import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ServerVersion.version;

public class ClusterDrivers implements AutoCloseable
{
    private final String user;
    private final String password;
    private final Map<ClusterMember,Driver> membersWithDrivers;
    private ClusterMemberRoleDiscovery discovery;

    public ClusterDrivers( String user, String password )
    {
        this.user = user;
        this.password = password;
        this.membersWithDrivers = new ConcurrentHashMap<>();
    }

    public Driver getDriver( ClusterMember member )
    {
        final Driver driver = membersWithDrivers.computeIfAbsent( member, this::createDriver );
        if ( discovery == null )
        {
            discovery = ClusterMemberRoleDiscoveryFactory.newInstance( version( driver ) );
        }
        return driver;
    }

    public ClusterMemberRoleDiscovery getDiscovery()
    {
        return discovery;
    }

    @Override
    public void close()
    {
        for ( Driver driver : membersWithDrivers.values() )
        {
            driver.close();
        }
    }

    private Driver createDriver( ClusterMember member )
    {
        return GraphDatabase.driver( member.getBoltUri(), AuthTokens.basic( user, password ), driverConfig() );
    }

    private static Config driverConfig()
    {
        return Config.builder()
                .withLogging( DEV_NULL_LOGGING )
                .withoutEncryption()
                .withMaxConnectionPoolSize( 1 )
                .withConnectionLivenessCheckTimeout( 0, TimeUnit.MILLISECONDS )
                .withEventLoopThreads( 1 )
                .build();
    }
}
