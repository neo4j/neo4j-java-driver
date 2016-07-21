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
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import static org.neo4j.driver.internal.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.v1.util.FileTools.updateProperties;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    private static Neo4jRunner globalInstance;

    private static final boolean debug = Boolean.getBoolean( "neo4j.runner.debug" );

    public static final String NEORUN_START_ARGS = System.getProperty( "neorun.start.args" );
    public static final URI DEFAULT_URI = URI.create( "bolt://localhost:7687" );
    public static final BoltServerAddress DEFAULT_ADDRESS = BoltServerAddress.from( DEFAULT_URI );
    private Driver driver;
    private Neo4jSettings currentSettings = Neo4jSettings.DEFAULT_SETTINGS;

    public static final String NEO4J_HOME = new File("../target/neo4j/neo4jhome").getAbsolutePath();
    private static final String NEORUN_PATH = new File("../neokit/neorun.py").getAbsolutePath();
    private static final String NEO4J_CONF = new File( NEO4J_HOME, "conf/neo4j.conf" ).getAbsolutePath();

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalRunner() throws IOException
    {
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
            startNeo4j();
        }
        finally
        {
            // Make sure we stop on JVM exit even if start failed
            installShutdownHook();
        }
    }

    public void ensureRunning(Neo4jSettings neo4jSettings) throws IOException, InterruptedException
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
        return driver;
    }

    private void startNeo4j() throws IOException
    {
        // this is required for windows as python scripts cannot delete the file when it is used by driver tests
        deleteDefaultKnownCertFileIfExists();

        int processStatus = runCommand( "python", NEORUN_PATH, "--start=" + NEO4J_HOME );
        if (processStatus != 0) // not success
        {
            throw new IOException( "Failed to start neo4j server." );
        }
        driver = GraphDatabase.driver( DEFAULT_URI /* default encryption REQUIRED_NON_LOCAL */ );
    }

    public synchronized void stopNeo4j() throws IOException
    {
        if(serverStatus() == ServerStatus.OFFLINE)
        {
            return;
        }
        if(driver != null)
        {
            driver.close();
            driver = null;
        }

        int processStatus = runCommand( "python", NEORUN_PATH, "--stop=" + NEO4J_HOME );
        if( processStatus != 0 )
        {
            throw new IOException( "Failed to stop neo4j server." );
        }
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
    public void restartNeo4j(Neo4jSettings neo4jSettings) throws IOException
    {
        if( updateServerSettings( neo4jSettings ) ) // needs to update server setting files
        {
            forceToRestart();
        }
    }

    @SuppressWarnings("LoopStatementThatDoesntLoop")
    private int runCommand( String... cmd ) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder().inheritIO();
        Map<String,String> env = System.getenv();
        pb.environment().put( "JAVA_HOME",
                // This driver is built to work with multiple java versions.
                // Neo4j, however, works with a specific version of Java. This allows
                // specifying which Java version to use for Neo4j separately from which
                // version to use for the driver tests.
                env.containsKey( "NEO4J_JAVA" ) ? env.get( "NEO4J_JAVA" ) :
                System.getProperties().getProperty( "java.home" ) );
        if( NEORUN_START_ARGS != null )
        {
            // overwrite the env var in the sub process if the system property is specified
            pb.environment().put( "NEORUN_START_ARGS", NEORUN_START_ARGS );
        }
        Process process = pb.command( cmd ).start();
        while (true)
        {
            try
            {
                return process.waitFor();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
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

        File oldFile = new File( NEO4J_CONF );
        try
        {
            debug( "Changing server properties file (for next start): " + oldFile.getCanonicalPath() );
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
                updateServerSettings( Neo4jSettings.TEST_SETTINGS );
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

