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

import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;

import static org.junit.Assume.assumeTrue;

public class ClusterRule extends ExternalResource
{
    private static final Path CLUSTER_DIR = Paths.get( "target", "test-cluster" ).toAbsolutePath();
    private static final String PASSWORD = "test";
    private static final int INITIAL_PORT = 20_000;

    // todo: neo4j version should come from environment variables
    private static final String NEO4J_VERSION = "3.1.0";
    // todo: should be possible to configure (dynamically add/remove) cores and read replicas
    private static final int CORE_COUNT = 3;
    private static final int READ_REPLICA_COUNT = 2;

    public Cluster getCluster()
    {
        return SharedCluster.get();
    }

    public AuthToken getDefaultAuthToken()
    {
        return AuthTokens.basic( "neo4j", PASSWORD );
    }

    @Override
    protected void before() throws Throwable
    {
        assumeTrue( "BoltKit cluster support unavailable", ClusterControl.boltKitClusterAvailable() );

        if ( !SharedCluster.exists() )
        {
            deleteClusterDir();

            SharedCluster.install( NEO4J_VERSION, CORE_COUNT, READ_REPLICA_COUNT, PASSWORD, INITIAL_PORT, CLUSTER_DIR );

            try
            {
                SharedCluster.start();
            }
            catch ( Throwable startError )
            {
                try
                {
                    SharedCluster.kill();
                }
                catch ( Throwable killError )
                {
                    startError.addSuppressed( killError );
                }
                finally
                {
                    SharedCluster.remove();
                }
                throw startError;
            }
            finally
            {
                addShutdownHookToStopCluster();
            }
        }
    }

    @Override
    protected void after()
    {
        Cluster cluster = getCluster();
        cluster.startOfflineMembers();
        cluster.deleteData();
    }

    private static void addShutdownHookToStopCluster()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    SharedCluster.kill();
                }
                catch ( Throwable t )
                {
                    System.err.println( "Cluster stopping shutdown hook failed" );
                    t.printStackTrace();
                }
            }
        } );
    }

    private static void deleteClusterDir()
    {
        delete( CLUSTER_DIR );
    }

    private static void delete( final Path path )
    {
        try
        {
            if ( !Files.exists( path ) )
            {
                return;
            }

            Files.walkFileTree( path, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile( Path file, BasicFileAttributes attributes ) throws IOException
                {
                    Files.delete( file );
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory( Path dir, IOException error ) throws IOException
                {
                    if ( error != null )
                    {
                        return FileVisitResult.TERMINATE;
                    }
                    Files.delete( dir );
                    return FileVisitResult.CONTINUE;
                }
            } );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to delete '" + path + "'", e );
        }
    }
}
