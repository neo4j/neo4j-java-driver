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
package org.neo4j.driver.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.neo4j.driver.util.cc.CommandLineException;

import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_CERT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_IMPORT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_LOG_DIR;

public class Neo4jDockerProcess
{
    private Process process;
    private final ArrayList<String> commands;
    private final String DEFAULT_NEO4J_HOME_PATH = "/var/lib/neo4j";

    public Neo4jDockerProcess( String neo4jVersion, String user, String password, int port, File certsFolder, File logsFolder, File importFolder )
    {
        String commandStr = String.format( "bolt server -v -i %s -a %s:%s -B %s", neo4jVersion, user, password, port );
        commands = new ArrayList<>( Arrays.asList( commandStr.split( "\\s+" ) ) );

        ensureFolderExists( certsFolder );
        commands.add( "--volume" );
        commands.add( String.format( "%s:%s", certsFolder.getAbsolutePath(),
                new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_CERT_DIR ).getAbsolutePath() ) );

        ensureFolderExists( logsFolder );
        commands.add( "--volume" );
        commands.add( String.format( "%s:%s", logsFolder.getAbsolutePath(),
                new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_LOG_DIR ).getAbsolutePath() ) );

        ensureFolderExists( importFolder );
        commands.add( "--volume" );
        commands.add( String.format( "%s:%s", importFolder.getAbsolutePath(),
                new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_IMPORT_DIR ).getAbsolutePath() ) );
    }

    private void ensureFolderExists( File folder )
    {
        if( !folder.exists() )
        {
            folder.mkdirs();
        }
    }

    /**
     * Start the process with the given settings without waiting for the start to finish.
     */
    public void start( Neo4jSettings settings )
    {
        if( isStarted() )
        {
            throw new CommandLineException( "Process is already started." );
        }

        final List<String> fullCommands = appendSettings( settings, this.commands );

        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder().command( fullCommands );
            ProcessEnvConfigurator.configure( processBuilder );
//            processBuilder.redirectError( Redirect.INHERIT );
            process = processBuilder.start();
            waitForServerOnline();
        }
        catch ( IOException e )
        {
            throw new CommandLineException( "Failed to start process", e );
        }
    }

    private void waitForServerOnline()
    {
        Scanner scanner = new Scanner( process.getInputStream() );
        boolean started = false;
        while ( !started && scanner.hasNextLine() )
        {
            String line = scanner.nextLine();
            System.out.println( line );
            if ( line.equals( "Press Ctrl+C to exit" ) )
            {
                started = true;
            }
        }
        if ( !started )
        {
            throw new RuntimeException( "Failed to start Neo4j." );
        }
    }

    private static List<String> appendSettings( Neo4jSettings settings, List<String> originalCommands )
    {
        final Map<String,String> properties = settings.propertiesMap();
        if( properties.size() == 0 )
        {
            return originalCommands;
        }

        List<String> commands = new ArrayList<>( originalCommands );
        for ( String key : properties.keySet() )
        {
            commands.add( "--env" );
            commands.add( String.format( "%s=%s", key, properties.get( key ) ) );
        }
        return commands;
    }

    public void update( Neo4jSettings settings )
    {
        if ( isStarted() )
        {
            stop();
            waitForProcessToExit();
            start( settings );
        }
        else
        {
            start( settings );
        }
    }

    private boolean isStarted()
    {
        return process != null && process.isAlive();
    }

    public int waitForProcessToExit()
    {
        try
        {
            return process.waitFor();
        }
        catch ( InterruptedException e )
        {
            throw new CommandLineException( "Interrupted while waiting for process to terminate.", e );
        }
    }

    /**
     * Stop the process without waiting for the termination.
     */
    public void stop()
    {
        if ( isStarted() )
        {
            process.destroy();
        }
    }

    /**
     * Kill the process without waiting for kill result.
     */
    public void kill()
    {
        if ( isStarted() )
        {
            process.destroyForcibly();
        }
    }
}
