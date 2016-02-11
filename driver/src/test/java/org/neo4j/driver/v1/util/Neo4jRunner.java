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

import static junit.framework.TestCase.assertFalse;

import static org.neo4j.driver.internal.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.v1.util.FileTools.deleteRecursively;
import static org.neo4j.driver.v1.util.FileTools.updateProperties;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    public static final String DEFAULT_URL = "bolt://localhost:7687";

    private static Neo4jRunner globalInstance;

    private static final boolean externalServer = false; //Boolean.getBoolean( "neo4j.useExternalServer" );

    private Neo4jSettings cachedSettings = Neo4jSettings.DEFAULT;
    private Driver currentDriver;
    private boolean staleDriver;

    private Neo4jInstaller installer = Neo4jInstaller.Neo4jInstallerFactory.create();

    public static void main( String... args ) throws Exception
    {
        Neo4jRunner neo4jRunner = new Neo4jRunner();
        neo4jRunner.startServerOnEmptyDatabase();
        neo4jRunner.stopServerIfRunning();
    }

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
        if ( canControlServer() )
        {
            installer.installNeo4j();
            // Install default settings
            updateServerSettingsFile();

            // Reset driver to match default settings
            resetDriver();

            // Make sure we stop on JVM exit
            installShutdownHook();
        }
    }

    public synchronized void restartServerOnEmptyDatabase() throws Exception
    {
        restartServerOnEmptyDatabase( cachedSettings );
    }

    public synchronized void restartServerOnEmptyDatabase( Neo4jSettings settingsUpdate ) throws Exception
    {
        if ( canControlServer() )
        {
            stopServerIfRunning();
            startServerOnEmptyDatabase( settingsUpdate );
        }
    }

    public synchronized void startServerOnEmptyDatabase() throws Exception
    {
        startServerOnEmptyDatabase( cachedSettings );
    }

    public synchronized void startServerOnEmptyDatabase( Neo4jSettings settingsUpdate ) throws Exception
    {
        if ( canControlServer() )
        {
            assertFalse( "A server instance is already running", serverStatus() == ServerStatus.ONLINE );
            updateServerSettings( settingsUpdate );
            doStartServerOnEmptyDatabase();
        }
    }

    public synchronized boolean startServerOnEmptyDatabaseUnlessRunning( Neo4jSettings settingsUpdate ) throws Exception
    {
        if ( canControlServer() )
        {
            ServerStatus status = serverStatus();
            switch ( status )
            {
                case OFFLINE:
                    updateServerSettings( settingsUpdate );
                    doStartServerOnEmptyDatabase();
                    return true;

                case ONLINE:
                    if ( updateServerSettings( settingsUpdate ) )
                    {
                        doStopServer();
                        doStartServerOnEmptyDatabase();
                        return true;
                    }
                    else
                    {
                        return false;
                    }
            }
        }
        return true;
    }

    private void doStartServerOnEmptyDatabase() throws Exception
    {
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

    public synchronized void stopServerIfRunning() throws IOException, InterruptedException
    {
        if ( serverStatus() == ServerStatus.ONLINE )
        {
            doStopServer();
        }
    }

    private void doStopServer() throws IOException, InterruptedException
    {
        if ( !tryStopServer() )
        {
            throw new IllegalStateException( "Failed to stop server" );
        }
    }

    private synchronized boolean tryStopServer() throws IOException, InterruptedException
    {
        debug( "Trying to stop server at %s", Neo4jInstaller.neo4jHomeDir.getCanonicalPath() );

        if ( canControlServer() )
        {
            int exitCode = installer.stopNeo4j();
            if ( exitCode == 0 )
            {
                awaitServerStatusOrFail( ServerStatus.OFFLINE );
                return true;
            }
        }
        return false;
    }

    private boolean updateServerSettings( Neo4jSettings settingsUpdate )
    {
        if ( cachedSettings == null )
        {
            cachedSettings = settingsUpdate;
        }
        else
        {
            Neo4jSettings updatedSettings = cachedSettings.updateWith( settingsUpdate );
            if ( cachedSettings.equals( updatedSettings ) )
            {
                return false;
            }
            else
            {
                cachedSettings = updatedSettings;
            }
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
        Map<String, Object> propertiesMap = cachedSettings.propertiesMap();
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
            System.out.println( "Failed to update properties" );
            throw new RuntimeException( e );
        }
    }

    public boolean canControlServer()
    {
        return !externalServer;
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
        Driver oldDriver = currentDriver;
        try
        {
            debug( "Resetting driver" );
            currentDriver = new Driver( serverURI(), serverConfig() );
            staleDriver = false;
        }
        finally
        {
            if ( oldDriver != null )
            {
                oldDriver.close();
            }
        }
    }

    private Config serverConfig()
    {
        Config config = Config.defaultConfig();
        if( cachedSettings.isUsingTLS() )
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
                stopServerIfRunning();
                cachedSettings = Neo4jSettings.DEFAULT;
                updateServerSettingsFile();
                installer.uninstallNeo4j();
                debug("Finished shutdown hook");
            }
            catch ( Exception e )
            {
                // cannot help you anything sorry
                System.out.println("Failed to shutdown neo4j server");
                e.printStackTrace();
            }
            }
        } ) );
    }

    public Driver driver()
    {
        return currentDriver;
    }

    private enum ServerStatus {
        ONLINE, OFFLINE
    }

    private static boolean DEBUG = isEnabled( "DEBUG_NEO4J_RUNNER" );

    static void debug( String text, Object... args )
    {
        if ( DEBUG )
        {
            System.err.println( "Neo4jRunner: " + String.format( text, args ) );
        }
    }

    private static boolean isEnabled( String envVarName )
    {
        String value = System.getenv( envVarName );
        if ( value != null )
        {
            value = value.trim();
            return value.equals( "1" ) || value.equalsIgnoreCase( "true" );
        }
        else
        {
            return false;
        }
    }
}

