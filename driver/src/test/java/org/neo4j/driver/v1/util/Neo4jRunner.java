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
package org.neo4j.driver.v1.util;

import java.io.File;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.v1.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.v1.util.FileTools.updateProperties;
import static org.neo4j.driver.v1.util.cc.CommandLineUtil.boltKitAvailable;
import static org.neo4j.driver.v1.util.cc.CommandLineUtil.executeCommand;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    private static Neo4jRunner globalInstance;

    private static final boolean debug = true;

    private static final String DEFAULT_NEOCTRL_ARGS = "-e 3.1.2";
    public static final String NEOCTRL_ARGS = System.getProperty( "neoctrl.args", DEFAULT_NEOCTRL_ARGS );
    public static final URI DEFAULT_URI = URI.create( "bolt://localhost:7687" );
    public static final BoltServerAddress DEFAULT_ADDRESS = BoltServerAddress.from( DEFAULT_URI );
    private Driver driver;
    private Neo4jSettings currentSettings = Neo4jSettings.TEST_SETTINGS;

    public static final String TARGET_DIR = new File( "../target" ).getAbsolutePath();
    private static final String NEO4J_DIR = new File(  TARGET_DIR, "neo4j" ).getAbsolutePath();
    public static String NEO4J_HOME;

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalRunner() throws IOException
    {
        assumeTrue( "BoltKit support unavailable", boltKitAvailable() );
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
        }
        return globalInstance;
    }

    private Neo4jRunner() throws IOException
    {
        try
        {
            installNeo4j();
            startNeo4j();
        }
        finally
        {
            // Make sure we stop on JVM exit even if start failed
            installShutdownHook();
        }
    }

    public void ensureRunning( Neo4jSettings neo4jSettings ) throws IOException, InterruptedException
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

    public Driver driver()
    {
        if ( driver == null )
        {
            driver = GraphDatabase.driver( DEFAULT_URI );
        }
        return driver;
    }

    private void installNeo4j() throws IOException
    {
        // this is required for windows as python scripts cannot delete the file when it is used by driver tests
        deleteDefaultKnownCertFileIfExists();

        List<String> commands = new ArrayList<>();
        commands.add( "neoctrl-install" );
        String[] split = NEOCTRL_ARGS.trim().split( "\\s+" );
        commands.addAll( asList( split ) );
        commands.add( NEO4J_DIR );

        NEO4J_HOME = executeCommand( commands ).trim();
        updateServerSettingsFile();

        debug( "Installed server at `%s`.", NEO4J_HOME );
    }

    private void startNeo4j() throws IOException
    {
        debug( "Starting server..." );
        executeCommand( "neoctrl-create-user", NEO4J_HOME, "neo4j", "neo4j" );
        executeCommand( "neoctrl-start", NEO4J_HOME );
        debug( "Server started." );
    }

    public synchronized void stopNeo4j() throws IOException
    {
        if( serverStatus() == ServerStatus.OFFLINE )
        {
            return;
        }
        if( driver != null )
        {
            driver.close();
            driver = null;
        }

        debug( "Stopping server..." );
        executeCommand( "neoctrl-stop", NEO4J_HOME );
        debug( "Server stopped." );
    }

    public void forceToRestart() throws IOException
    {
        stopNeo4j();
        startNeo4j();
    }

    /**
     * Restart the server with default testing server configuration
     * @throws IOException
     */
    public void restartNeo4j() throws IOException
    {
        restartNeo4j( Neo4jSettings.TEST_SETTINGS );
    }

    /**
     * Will only restart the server if any configuration changes happens
     * @param neo4jSettings
     * @throws IOException
     */
    public void restartNeo4j( Neo4jSettings neo4jSettings ) throws IOException
    {
        if( updateServerSettings( neo4jSettings ) ) // needs to update server setting files
        {
            forceToRestart();
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
            soChannel.connect( DEFAULT_ADDRESS.toSocketAddress() );
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
        updateServerSettingsFile();
        return true;
    }

    /**
     * Write updated neo4j settings into neo4j-server.properties for use by the next start
     */
    private void updateServerSettingsFile()
    {
        Map<String, String> propertiesMap = currentSettings.propertiesMap();

        if ( propertiesMap.isEmpty() )
        {
            return;
        }

        File oldFile = new File( NEO4J_HOME, "conf/neo4j.conf" );
        try
        {
            debug( "Changing server properties file (for next start): %s", oldFile.getCanonicalPath() );
            for ( Map.Entry<String, String> property : propertiesMap.entrySet() )
            {
                String name = property.getKey();
                Object value = property.getValue();
                debug( "%s=%s", name, value );
            }

            updateProperties( oldFile, propertiesMap, currentSettings.excludes() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void installShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
        {
            @Override
            public void run()
            {
            try
            {
                debug("Starting shutdown hook");
                stopNeo4j();
                debug("Finished shutdown hook");
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
            }
        } ) );
    }

    static void debug( String text, Object... args )
    {
        if ( debug )
        {
            System.out.println( String.format( text, args ) );
        }
    }
}

