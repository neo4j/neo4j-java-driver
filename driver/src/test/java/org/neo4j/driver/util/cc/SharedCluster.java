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
package org.neo4j.driver.util.cc;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.util.Neo4jSettings;

import static org.neo4j.driver.util.Neo4jRunner.CLUSTER_DIR;
import static org.neo4j.driver.util.Neo4jRunner.debug;

final class SharedCluster
{
    private Cluster clusterInstance;
    private ClusterDockerProcess process;

    public SharedCluster()
    {
    }

    Cluster get()
    {
        assertClusterExists();
        return clusterInstance;
    }

    void remove()
    {
        assertClusterExists();
        clusterInstance.close();
        clusterInstance = null;
    }

    boolean exists()
    {
        return clusterInstance != null;
    }

    void install( String neo4jVersion, int cores, int readReplicas, String user, String password, int boltPort, int httpPort, File logDir )
    {
        assertClusterDoesNotExist();
        process = new ClusterDockerProcess( neo4jVersion, cores, readReplicas, user, password, boltPort, httpPort, logDir );
        clusterInstance = new Cluster( user, password );
    }

    void start() throws ClusterUnavailableException
    {
        assertClusterExists();
        try
        {
            final List<String> lines = process.start( Neo4jSettings.EMPTY_SETTINGS );
            Set<ClusterMember> members = parseStartCommandOutput( lines );
            clusterInstance = clusterInstance.withMembers( members );
            debug( "Cluster started: %s.", members );
        }
        catch ( ClusterUnavailableException e )
        {
            stop();
            throw e;
        }
    }

    void stop()
    {
        try
        {
            if ( clusterInstance != null )
            {
                clusterInstance.close();
            }
        }
        finally
        {
            if ( process != null )
            {
                process.stop();
            }
        }
        debug( "Cluster at `%s` stopped.", CLUSTER_DIR );
    }

    private static Set<ClusterMember> parseStartCommandOutput( List<String> lines ) throws ClusterUnavailableException
    {
        Set<ClusterMember> result = new HashSet<>();

        for ( String line : lines )
        {
            if( line.startsWith( "BOLT_SERVER_ADDR" ) )
            {
                String members = line.substring( "BOLT_SERVER_ADDR='".length(), line.length() - 1 );
                for( String member : members.split( "\\s+" ) )
                {
                    final String[] split = member.split( "=" );
                    if ( split.length != 2 )
                    {
                        throw new IllegalArgumentException( String.format( "Wrong command output found %s", line ) );
                    }
                    result.add( new ClusterMember( split[0], split[1] ) );
                }
                break;
            }
        }

        if ( result.isEmpty() )
        {
            throw new ClusterUnavailableException( "No cluster members" );
        }

        return result;
    }

    private void assertClusterExists()
    {
        if ( clusterInstance == null )
        {
            throw new IllegalStateException( "Shared cluster does not exist" );
        }
    }

    private void assertClusterDoesNotExist()
    {
        if ( clusterInstance != null )
        {
            throw new IllegalStateException( "Shared cluster already exists" );
        }
    }

    public void stop( ClusterMember member )
    {
        process.stop( member.getName() );
    }

    public void startAll()
    {
        process.startAll();
    }
}
