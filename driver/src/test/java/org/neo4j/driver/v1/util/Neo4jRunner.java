/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.net.URI;
import java.util.Map;

import org.neo4j.driver.internal.connector.socket.SocketClient;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.String.format;

import static org.neo4j.driver.internal.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.v1.util.FileTools.deleteRecursively;
import static org.neo4j.driver.v1.util.FileTools.updateProperties;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    private enum ServerStatus
    {
        ONLINE, OFFLINE
    }

    public static final String DEFAULT_URL = "bolt://localhost:7687";
    private static final boolean debug = Boolean.getBoolean( "neo4j.runner.debug" );

    private static Neo4jRunner globalInstance;

    private Neo4jSettings currentSettings = Neo4jSettings.DEFAULT;
    private Driver currentDriver;
    private boolean staleDriver;

    private Neo4jInstaller installer = Neo4jInstaller.Neo4jInstallerFactory.create();

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalRunner() throws Exception
    {
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
        }
        return globalInstance;
    }

    private Neo4jRunner() throws Exception
    {
        if( serverStatus() == ServerStatus.ONLINE )
        {
            // TODO: We should just pick a free port instead
            throw new IllegalStateException( "Cannot run tests, because a Neo4j Database is already running on this " +
                                             "system." );
        }
        installer.installNeo4j();
        // Install default settings
        updateServerSettingsFile();

        // Reset driver to match default settings
        resetDriver();

        // Make sure we stop on JVM exit
        installShutdownHook();
    }

    public synchronized void restart() throws Exception
    {
        stop();
        ensureRunning( Neo4jSettings.DEFAULT );
    }

    public synchronized boolean ensureRunning( Neo4jSettings withSettings ) throws Exception
    {
        ServerStatus status = serverStatus();
        switch ( status )
        {
            case OFFLINE:
                clear( withSettings );
                return true;

            case ONLINE:
                if ( updateServerSettings( withSettings ) )
                {
                    clear( currentSettings );
                    return true;
                }
                else
                {
                    return false;
                }
        }
        return true;
    }

    public synchronized void stop() throws IOException, InterruptedException
    {
        if ( serverStatus() == ServerStatus.ONLINE )
        {
            debug( "Trying to stop server at %s", Neo4jInstaller.neo4jHomeDir.getCanonicalPath() );

            if ( installer.stopNeo4j() == 0 )
            {
                awaitServerStatusOrFail( ServerStatus.OFFLINE );
            }
            else
            {
                throw new IllegalStateException( "Failed to stop server" );
            }
        }
    }

    public Driver driver()
    {
        return currentDriver;
    }

    /** Delete all database files, apply the specified configuration and restart */
    private void clear( Neo4jSettings config ) throws Exception
    {
        stop();
        updateServerSettings( config );

        debug( "Deleting database at: %s", Neo4jInstaller.dbDir.getCanonicalPath() );

        deleteRecursively( Neo4jInstaller.dbDir );
        deleteDefaultKnownCertFileIfExists();

        debug( "Starting server at: ", Neo4jInstaller.neo4jHomeDir.getCanonicalPath() );

        if ( installer.startNeo4j() != 0 )
        {
            throw new IllegalStateException( "Failed to start server" );
        }
        awaitServerStatusOrFail( ServerStatus.ONLINE );

        if ( staleDriver )
        {
            resetDriver();
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
        staleDriver = true;
        return true;
    }

    /**
     * Write updated neo4j settings into neo4j-server.properties for use by the next start
     */
    private void updateServerSettingsFile()
    {
        Map<String, Object> propertiesMap = currentSettings.propertiesMap();
        if ( propertiesMap.isEmpty() )
        {
            return;
        }

        File oldFile = new File( Neo4jInstaller.neo4jHomeDir, "conf/neo4j.conf" );
        try
        {
            debug( "Changing server properties file (for next start): " + oldFile.getCanonicalPath() );
            for ( Map.Entry<String, Object> property : propertiesMap.entrySet() )
            {
                String name = property.getKey();
                Object value = property.getValue();
                debug( "%s=%s", name, value );
            }

            updateProperties( oldFile, propertiesMap );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void awaitServerStatusOrFail( ServerStatus goalStatus ) throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        for (; ;)
        {
            if ( serverStatus() == goalStatus )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }

            if ( System.currentTimeMillis() > timeout )
            {
                throw new RuntimeException( format(
                        "Waited for 30 seconds for server to become %s but failed, " +
                        "timing out to avoid blocking forever.", goalStatus ) );
            }
        }
    }

    private ServerStatus serverStatus() throws IOException, InterruptedException
    {
        try
        {
            URI uri = serverURI();
            Config config = serverConfig();
            SocketClient client = new SocketClient( uri.getHost(), uri.getPort(), config, new DevNullLogger() );
            client.start();
            client.stop();
            return ServerStatus.ONLINE;
        }
        catch ( ClientException e )
        {
            return ServerStatus.OFFLINE;
        }
    }

    private void resetDriver() throws Exception
    {
        if( currentDriver != null )
        {
            currentDriver.close();
        }
        currentDriver = new Driver( serverURI(), serverConfig() );
        staleDriver = false;
    }

    private Config serverConfig()
    {
        Config config = Config.defaultConfig();
        if( currentSettings.isUsingTLS() )
        {
            config = Config.build().withTlsEnabled( true ).toConfig();
        }
        return config;
    }

    private URI serverURI()
    {
        return URI.create( DEFAULT_URL );
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
                stop();
                currentSettings = Neo4jSettings.DEFAULT;
                updateServerSettingsFile();
                installer.uninstallNeo4j();
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
            System.err.println( "Neo4jRunner: " + String.format( text, args ) );
        }
    }
}

