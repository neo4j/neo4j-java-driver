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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.util.Neo4jRunner;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.util.Neo4jRunner.PASSWORD;
import static org.neo4j.driver.util.Neo4jRunner.TARGET_DIR;
import static org.neo4j.driver.util.Neo4jRunner.USER;
import static org.neo4j.driver.util.cc.CommandLineUtil.boltKitAvailable;

public class ClusterExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback
{
    private static final Path CLUSTER_DIR = Paths.get( TARGET_DIR, "test-cluster" ).toAbsolutePath();
    private static final int INITIAL_PORT = 20_000;

    public static final int CORE_COUNT = 3;
    public static final int READ_REPLICA_COUNT = 2;

    public Cluster getCluster()
    {
        return SharedCluster.get();
    }

    public AuthToken getDefaultAuthToken()
    {
        return AuthTokens.basic( USER, PASSWORD );
    }

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        assumeTrue( boltKitAvailable(), "BoltKit cluster support unavailable" );

        stopSingleInstanceDatabase();

        if ( !SharedCluster.exists() )
        {
            SharedCluster.install( parseNeo4jVersion(),
                    CORE_COUNT, READ_REPLICA_COUNT, PASSWORD, INITIAL_PORT, CLUSTER_DIR );

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

        getCluster().deleteData();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        Cluster cluster = getCluster();
        cluster.startOfflineMembers();
        cluster.deleteData();
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        if ( SharedCluster.exists() )
        {
            try
            {
                SharedCluster.stop();
            }
            finally
            {
                SharedCluster.remove();
            }
        }
    }

    private static String parseNeo4jVersion()
    {
        String[] split = Neo4jRunner.NEOCTRL_ARGS.split( "\\s+" );
        return split[split.length - 1];
    }

    private static void stopSingleInstanceDatabase() throws IOException
    {
        if ( Neo4jRunner.globalRunnerExists() )
        {
            Neo4jRunner.getOrCreateGlobalRunner().stopNeo4j();
        }
    }

    private static void addShutdownHookToStopCluster()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( () ->
        {
            try
            {
                if ( SharedCluster.exists() )
                {
                    SharedCluster.kill();
                }
            }
            catch ( Throwable t )
            {
                System.err.println( "Cluster stopping shutdown hook failed" );
                t.printStackTrace();
            }
        } ) );
    }
}
