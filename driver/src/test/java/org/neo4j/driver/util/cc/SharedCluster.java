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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static java.lang.System.lineSeparator;
import static org.neo4j.driver.util.Neo4jRunner.debug;

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
        clusterInstance.close();
        clusterInstance = null;
    }

    static boolean exists()
    {
        return clusterInstance != null;
    }

    static void install( String neo4jVersion, int cores, int readReplicas, String password, int port, Path path )
    {
        assertClusterDoesNotExist();
        if ( Files.isDirectory( path ) )
        {
            debug( "Found and using cluster installed at `%s`.", path );
        }
        else
        {
            ClusterControl.installCluster( neo4jVersion, cores, readReplicas, password, port, path );
            debug( "Downloaded cluster at `%s`.", path );
        }
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
            debug( "Cluster started: %s.", members );
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
        debug( "Cluster member at `%s` started.", member );
    }

    static void stop()
    {
        assertClusterExists();
        ClusterControl.stopCluster( clusterInstance.getPath() );
        debug( "Cluster at `%s` stopped.", clusterInstance.getPath() );
    }

    static void stop( ClusterMember member )
    {
        assertClusterExists();
        ClusterControl.stopClusterMember( member.getPath() );
        debug( "Cluster member at `%s` stopped.", member.getPath() );
    }

    static void kill()
    {
        assertClusterExists();
        ClusterControl.killCluster( clusterInstance.getPath() );
        debug( "Cluster at `%s` killed.", clusterInstance.getPath() );
    }

    static void kill( ClusterMember member )
    {
        assertClusterExists();
        ClusterControl.killClusterMember( member.getPath() );
        debug( "Cluster member at `%s` killed.", member.getPath() );
    }

    private static Set<ClusterMember> parseStartCommandOutput( String output )
    {
        Set<ClusterMember> result = new HashSet<>();

        String[] lines = output.split( lineSeparator() );
        for ( int i = 0; i < lines.length; i++ )
        {
            String line = lines[i].trim();
            if( line.isEmpty() )
            {
                // skip any empty lines
                continue;
            }
            String[] clusterMemberSplit = line.split( " " );
            if ( clusterMemberSplit.length != 3 )
            {
                throw new IllegalArgumentException( String.format(
                        "Wrong start command output found at line [%s]. " +
                        "Expected to have 'http_uri bolt_uri path' on each nonempty line. " +
                        "Command output:%n`%s`", i + 1, output ) );
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
