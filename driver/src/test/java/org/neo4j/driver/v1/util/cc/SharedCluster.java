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
package org.neo4j.driver.v1.util.cc;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static java.lang.System.lineSeparator;

final class SharedCluster
{
    private static Cluster clusterInstance;

    private SharedCluster()
    {
    }

    static Cluster get()
    {
        assertClusterExists();
        return clusterInstance;
    }

    static void remove()
    {
        assertClusterExists();
        clusterInstance = null;
    }

    static boolean exists()
    {
        return clusterInstance != null;
    }

    static void install( String neo4jVersion, int cores, int readReplicas, String password, int port, Path path )
    {
        assertClusterDoesNotExist();
        ClusterControl.installCluster( neo4jVersion, cores, readReplicas, password, port, path );
        clusterInstance = new Cluster( path, password );
    }

    static void start() throws ClusterUnavailableException
    {
        assertClusterExists();
        String output = ClusterControl.startCluster( clusterInstance.getPath() );
        Set<ClusterMember> members = parseStartCommandOutput( output );

        try
        {
            clusterInstance = clusterInstance.withMembers( members );
        }
        catch ( ClusterUnavailableException e )
        {
            kill();
            throw e;
        }
    }

    static void start( ClusterMember member )
    {
        assertClusterExists();
        ClusterControl.startClusterMember( member.getPath() );
    }

    static void stop()
    {
        assertClusterExists();
        ClusterControl.stopCluster( clusterInstance.getPath() );
    }

    static void stop( ClusterMember member )
    {
        assertClusterExists();
        ClusterControl.stopClusterMember( member.getPath() );
    }

    static void kill()
    {
        assertClusterExists();
        ClusterControl.killCluster( clusterInstance.getPath() );
    }

    static void kill( ClusterMember member )
    {
        assertClusterExists();
        ClusterControl.killClusterMember( member.getPath() );
    }

    private static Set<ClusterMember> parseStartCommandOutput( String output )
    {
        Set<ClusterMember> result = new HashSet<>();

        String[] lines = output.split( lineSeparator() );
        for ( String line : lines )
        {
            String[] clusterMemberSplit = line.split( " " );
            if ( clusterMemberSplit.length != 3 )
            {
                throw new IllegalArgumentException(
                        "Wrong start command output. " +
                        "Expected to have 'http_uri bolt_uri path' in line '" + line + "'" );
            }

            URI boltUri = URI.create( clusterMemberSplit[1] );
            Path path = Paths.get( clusterMemberSplit[2] );

            result.add( new ClusterMember( boltUri, path ) );
        }

        if ( result.isEmpty() )
        {
            throw new IllegalStateException( "No cluster members" );
        }

        return result;
    }

    private static void assertClusterExists()
    {
        if ( clusterInstance == null )
        {
            throw new IllegalStateException( "Shared cluster does not exist" );
        }
    }

    private static void assertClusterDoesNotExist()
    {
        if ( clusterInstance != null )
        {
            throw new IllegalStateException( "Shared cluster already exists" );
        }
    }
}
