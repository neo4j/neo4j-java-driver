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
import java.lang.ProcessBuilder.Redirect;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.util.cc.CommandLineException;

import static org.neo4j.driver.util.Neo4jRunner.debug;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_CERT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_IMPORT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_LOG_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_PLUGIN_DIR;

public abstract class DockerProcess
{
    protected Process process;
    private final ArrayList<String> commands;
    private static final String DEFAULT_NEO4J_HOME_PATH = "/var/lib/neo4j";
    private Scanner scanner;
    private static final Duration MAX_BLOCKING_TIME = Duration.ofMinutes( 5 );

    public DockerProcess( String baseCommandStr, File logFolder )
    {
        commands = new ArrayList<>( Arrays.asList( baseCommandStr.split( "\\s+" ) ) );
        mountSharedFolder( logFolder, new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_LOG_DIR ) );
    }

    public DockerProcess( String baseCommandStr, File logFolder, File certFolder, File importFolder, File pluginFolder )
    {
        this( baseCommandStr, logFolder );

        mountSharedFolder( certFolder, new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_CERT_DIR ) );
        mountSharedFolder( importFolder, new File( DEFAULT_NEO4J_HOME_PATH, DEFAULT_IMPORT_DIR ) );
        mountSharedFolder( pluginFolder, new File( "/" + DEFAULT_PLUGIN_DIR ) );
    }

    private void mountSharedFolder( File hostFolder, File dockerFolder )
    {
        ensureFolderExists( hostFolder );
        commands.add( "--volume" );
        commands.add( String.format( "%s:%s", hostFolder.getAbsolutePath(), dockerFolder.getAbsolutePath() ) );
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
    public List<String> start( Neo4jSettings settings )
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
            processBuilder.redirectError( Redirect.INHERIT );
            debug( "Running command: " + String.join( " ", fullCommands ) );
            process = processBuilder.start();
            scanner = new Scanner( process.getInputStream() );
            return waitForCommandReturn();
        }
        catch ( IOException e )
        {
            throw new CommandLineException( "Failed to start process", e );
        }
    }

    protected List<String> waitForCommandReturn()
    {
        long endTime = System.currentTimeMillis() + MAX_BLOCKING_TIME.toMillis();
        List<String> lines = new ArrayList<>();
        boolean started = false;
        while ( System.currentTimeMillis() < endTime && !started && scanner.hasNextLine() )
        {
            String line = scanner.nextLine();
            lines.add( line );
            System.out.println( line );
            if ( line.equals( "Type 'exit' to exit" ) )
            {
                started = true;
            }
        }
        if ( !started )
        {
            throw new RuntimeException( "Failed to run command." );
        }
        return lines;
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

    private int waitForProcessToExit()
    {
        Duration duration = MAX_BLOCKING_TIME;
        try
        {
            if ( process.waitFor( duration.toMillis(), TimeUnit.MILLISECONDS ) )
            {
                return process.waitFor();
            }
            throw new CommandLineException( "Service failed to shut down within " + duration.toMinutes() + "m." );
        }
        catch ( InterruptedException e )
        {
            throw new CommandLineException( "Interrupted while waiting for service to terminate.", e );
        }
    }

    /**
     * Stop the process without waiting for the termination.
     */
    public void stop()
    {
        if ( isStarted() )
        {
            try
            {
                process.getOutputStream().write( "exit\n".getBytes() );
                process.getOutputStream().flush();
                waitForProcessToExit();
            }
            catch ( IOException e )
            {
                process.destroy();
            }

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
