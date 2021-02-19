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

import java.nio.file.Path;

import static org.neo4j.driver.util.cc.CommandLineUtil.executeCommand;

final class ClusterControl
{
    private ClusterControl()
    {
    }

    static void installCluster( String neo4jVersion, int cores, int readReplicas, String password, int port,
            Path path )
    {
        executeCommand( "neoctrl-cluster", "install",
                "--cores", String.valueOf( cores ), "--read-replicas", String.valueOf( readReplicas ),
                "--password", password, "--initial-port", String.valueOf( port ),
                neo4jVersion, path.toString() );
    }

    static String startCluster( Path path )
    {
        return executeCommand( "neoctrl-cluster", "start", path.toString() );
    }

    static String startClusterMember( Path path )
    {
        return executeCommand( "neoctrl-start", path.toString() );
    }

    static void stopCluster( Path path )
    {
        executeCommand( "neoctrl-cluster", "stop", path.toString() );
    }

    static void stopClusterMember( Path path )
    {
        executeCommand( "neoctrl-stop", path.toString() );
    }

    static void killCluster( Path path )
    {
        executeCommand( "neoctrl-cluster", "stop", "--kill", path.toString() );
    }

    static void killClusterMember( Path path )
    {
        executeCommand( "neoctrl-stop", "--kill", path.toString() );
    }

}
