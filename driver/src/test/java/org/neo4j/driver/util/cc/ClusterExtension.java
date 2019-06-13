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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.util.DockerBasedNeo4jRunner;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.util.Neo4jRunner.CLUSTER_DIR;
import static org.neo4j.driver.util.Neo4jRunner.NEO4J_VERSION;
import static org.neo4j.driver.util.Neo4jRunner.PASSWORD;
import static org.neo4j.driver.util.Neo4jRunner.USER;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_LOG_DIR;
import static org.neo4j.driver.util.cc.CommandLineUtil.boltKitAvailable;
import static org.neo4j.driver.util.cc.CommandLineUtil.dockerRunning;

public class ClusterExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback
{
    private static final int INITIAL_PORT = 20_000;
    private static final int INITIAL_HTTP_PORT = 21_000;

    public static final int CORE_COUNT = 3;
    public static final int READ_REPLICA_COUNT = 2;

    private static SharedCluster cluster;

    public AuthToken getDefaultAuthToken()
    {
        return AuthTokens.basic( USER, PASSWORD );
    }

    private File logsDirectory()
    {
        return new File( CLUSTER_DIR, DEFAULT_LOG_DIR );
    }

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        assumeTrue( boltKitAvailable(), "BoltKit cluster support unavailable" );
        assumeTrue( dockerRunning(), "Docker support unavailable" );

        if ( cluster == null )
        {
            cluster = new SharedCluster();
            cluster.install( NEO4J_VERSION, CORE_COUNT, READ_REPLICA_COUNT, USER, PASSWORD, INITIAL_PORT, INITIAL_HTTP_PORT, logsDirectory() );

            try
            {
                cluster.start();
            }
            catch ( Throwable startError )
            {
                try
                {
                    cluster.stop();
                }
                catch ( Throwable stopError )
                {
                    startError.addSuppressed( stopError );
                }
                finally
                {
                    cluster.remove();
                }
                throw startError;
            }
            finally
            {
                addShutdownHookToStopCluster();
            }
        }

        getCluster().deleteData();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        cluster.startAll();
        Cluster cluster = getCluster();
        cluster.deleteData();
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        if ( cluster != null )
        {
            try
            {
                cluster.stop();
            }
            finally
            {
                cluster.remove();
                cluster = null;
            }
        }
    }

    private static void addShutdownHookToStopCluster()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( () ->
        {
            try
            {
                if ( cluster != null )
                {
                    cluster.stop();
                }
            }
            catch ( Throwable t )
            {
                System.err.println( "Cluster stopping shutdown hook failed" );
                t.printStackTrace();
            }
        } ) );
    }

    public Cluster getCluster()
    {
        return cluster.get();
    }

    public void stop( ClusterMember follower )
    {
        cluster.stop( follower );
    }
}
