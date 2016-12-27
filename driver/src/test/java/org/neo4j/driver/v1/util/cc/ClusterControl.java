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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.driver.v1.util.ProcessEnvConfigurator;

import static java.lang.System.lineSeparator;

final class ClusterControl
{
    private ClusterControl()
    {
    }

    static boolean boltKitClusterAvailable()
    {
        try
        {
            executeCommand( "neoctrl-cluster", "--help" );
            return true;
        }
        catch ( ClusterControlException e )
        {
            return false;
        }
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

    private static String executeCommand( String... command )
    {
        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder().command( command );
            ProcessEnvConfigurator.configure( processBuilder );
            return executeAndGetStdOut( processBuilder );
        }
        catch ( IOException e )
        {
            throw new ClusterControlException( "Error running command " + Arrays.toString( command ), e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new ClusterControlException( "Interrupted while waiting for command " +
                                               Arrays.toString( command ), e );
        }
    }

    private static String executeAndGetStdOut( ProcessBuilder processBuilder )
            throws IOException, InterruptedException
    {
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        String stdOut = asString( process.getInputStream() );
        String stdErr = asString( process.getErrorStream() );
        if ( exitCode != 0 )
        {
            throw new ClusterControlException( "Non-zero exit code\nSTDOUT:\n" + stdOut + "\nSTDERR:\n" + stdErr );
        }
        return stdOut;
    }

    private static String asString( InputStream input )
    {
        StringBuilder result = new StringBuilder();
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( input ) ) )
        {
            String line;
            while ( (line = reader.readLine()) != null )
            {
                result.append( line ).append( lineSeparator() );
            }
        }
        catch ( IOException e )
        {
            throw new ClusterControlException( "Unable to read from stream", e );
        }
        return result.toString();
    }
}
