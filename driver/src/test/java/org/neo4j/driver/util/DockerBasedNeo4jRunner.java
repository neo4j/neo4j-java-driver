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
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.util.Neo4jRunner.debug;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_CERT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_IMPORT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.DEFAULT_LOG_DIR;
import static org.neo4j.driver.util.cc.CommandLineUtil.boltKitAvailable;
import static org.neo4j.driver.util.cc.CommandLineUtil.dockerRunning;

/**
 * This class starts neo4j in local docker container in a separate process.
 */
public class DockerBasedNeo4jRunner implements Neo4jRunner
{
    private static DockerBasedNeo4jRunner globalInstance;

    private Neo4jDockerProcess neo4jProcess;
    private Neo4jSettings currentSettings = Neo4jSettings.TEST_SETTINGS;

    private Driver driver;
    private boolean restartDriver;

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized DockerBasedNeo4jRunner getOrCreateGlobalRunner() throws IOException
    {
        assumeTrue( boltKitAvailable(), "BoltKit support unavailable" );
        assumeTrue( dockerRunning(), "Docker is not running" );
        if ( globalInstance == null )
        {
            globalInstance = new DockerBasedNeo4jRunner();
        }

        return globalInstance;
    }

    public static synchronized boolean globalRunnerExists()
    {
        return globalInstance != null;
    }

    private DockerBasedNeo4jRunner() throws IOException
    {
        try
        {
            startNeo4j();
        }
        finally
        {
            // Make sure we stop on JVM exit even if start failed
            installShutdownHook();
        }
    }

    @Override
    public void ensureRunning( Neo4jSettings neo4jSettings )
    {
        ServerStatus status = serverStatus();
        switch( status )
        {
        case OFFLINE:
            updateServerSettings( neo4jSettings );
            startNeo4j();
            break;
        case ONLINE:
            restartNeo4j( neo4jSettings );
            break;
        }
    }

    @Override
    public Driver driver()
    {
        if ( restartDriver )
        {
            restartDriver = false;
            if ( driver != null )
            {
                driver.close();
                driver = null;
            }
        }

        if ( driver == null )
        {
            driver = GraphDatabase.driver( boltUri(), DEFAULT_AUTH_TOKEN, DEFAULT_DRIVER_CONFIG );
        }
        return driver;
    }

    @Override
    public void startNeo4j()
    {
        debug( "Starting server..." );
        neo4jProcess = new Neo4jDockerProcess( NEO4J_VERSION, USER, PASSWORD, boltPort(), certificatesDirectory(), logsDirectory(), importsDirectory() );
        neo4jProcess.start( currentSettings );
        debug( "Server started." );
    }

    @Override
    public synchronized void stopNeo4j()
    {
        if( serverStatus() == ServerStatus.OFFLINE )
        {
            return;
        }
        restartDriver = true;

        debug( "Stopping server..." );
        neo4jProcess.stop();
        neo4jProcess.waitForProcessToExit();
        debug( "Server stopped." );
    }

    @Override
    public void killNeo4j()
    {
        if ( serverStatus() == ServerStatus.OFFLINE )
        {
            return;
        }
        restartDriver = true;

        debug( "Killing server..." );
        neo4jProcess.kill();
        neo4jProcess.waitForProcessToExit();
        debug( "Server killed." );
    }

    @Override
    public void restartNeo4j()
    {
        neo4jProcess.update( currentSettings );
    }

    @Override
    public void restartNeo4j( Neo4jSettings neo4jSettings )
    {
        if( updateServerSettings( neo4jSettings ) ) // needs to update server setting files
        {
            neo4jProcess.update( currentSettings );
        }
    }

    private enum ServerStatus
    {
        ONLINE, OFFLINE
    }

    private ServerStatus serverStatus()
    {
        try
        {
            SocketChannel soChannel = SocketChannel.open();
            soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
            soChannel.connect( boltAddress().toSocketAddress() );
            soChannel.close();
            return ServerStatus.ONLINE;
        }
        catch ( IOException e )
        {
            return ServerStatus.OFFLINE;
        }
    }

    private boolean updateServerSettings( Neo4jSettings settingsUpdate )
    {
        Neo4jSettings updatedSettings = currentSettings.updateWith( settingsUpdate );
        if ( currentSettings.equals( updatedSettings ) )
        {
            return false;
        }
        else
        {
            currentSettings = updatedSettings;
        }
        return true;
    }

    private void installShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( () ->
        {
            try
            {
                debug( "Starting shutdown hook" );
                if ( driver != null )
                {
                    driver.close();
                }
                stopNeo4j();
                debug( "Finished shutdown hook" );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        } ) );
    }

    @Override
    public File certificatesDirectory()
    {
        return new File( NEO4J_DIR, DEFAULT_CERT_DIR );
    }

    @Override
    public File importsDirectory()
    {
        return new File( NEO4J_DIR, DEFAULT_IMPORT_DIR );
    }

    private File logsDirectory()
    {
        return new File( NEO4J_DIR, DEFAULT_LOG_DIR );
    }
}

