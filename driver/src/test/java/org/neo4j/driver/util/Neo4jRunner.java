/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.util;

import org.rauschig.jarchivelib.ArchiveStream;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.connector.socket.SocketClient;
import org.neo4j.driver.internal.logging.DevNullLogger;

import static junit.framework.TestCase.assertFalse;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    public static final String DEFAULT_URL = "neo4j://localhost:7687";

    private static Neo4jRunner globalInstance;
    private static boolean externalServer = Boolean.getBoolean( "neo4j.useExternalServer" );

    private static final String neo4jVersion = System.getProperty( "version", "3.0.0-alpha.LATEST" );
    private static final String neo4jLink = System.getProperty( "packageUri",
            String.format( "http://alpha.neotechnology.com.s3-website-eu-west-1.amazonaws.com/" +
                           "neo4j-enterprise-%s-unix.tar.gz", neo4jVersion ) );

    private final File neo4jDir = new File( "./target/neo4j" );
    private final File neo4jHome = new File( neo4jDir, neo4jVersion );
    private final File dataDir = new File( neo4jHome, "data" );
    private boolean isTLSEnabled;


    public static void main( String... args ) throws Exception
    {
        Neo4jRunner neo4jRunner = new Neo4jRunner();
        neo4jRunner.startServer();
        neo4jRunner.stopServer();
    }

    /** Global runner controlling a single server, used to avoid having to restart the server between tests */
    public static synchronized Neo4jRunner getOrCreateGlobalServer() throws IOException, InterruptedException
    {
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
            globalInstance.startServer();
        }
        return globalInstance;
    }

    public Neo4jRunner() throws IOException
    {
        if ( !externalServer )
        {
            if ( neo4jHome.exists() )
            {
                System.out.println( "Found an old Neo4j server in: " + neo4jHome.getAbsolutePath() + ". Deleting to " +
                                    "use a new one." );
                FileTools.deleteRecursively( neo4jHome );
            }

            // no neo4j exists

            // download neo4j server from a URL
            File neo4jTarball = new File( "./target/" + neo4jVersion + ".tar.gz" );
            ensureDownloaded( neo4jTarball, neo4jLink );

            // Untar the neo4j server
            extractTarball( neo4jTarball );

            File configFile = new File( neo4jHome, "conf/neo4j-server.properties" );
            FileTools.setProperty( configFile, "xx.ndp.enabled", "true" );

        }
    }

    private void extractTarball( File neo4jTarball ) throws IOException
    {
        System.out.println( "Extracting: " + neo4jTarball + " -> " + neo4jDir );
        Archiver archiver = ArchiverFactory.createArchiver( "tar", "gz" );

        archiver.extract( neo4jTarball, neo4jDir );

        // Rename the extracted file to something predictable (extracted folder may contain build number, date or so)
        try ( ArchiveStream stream = archiver.stream( neo4jTarball ) )
        {
            new File( neo4jDir, stream.getNextEntry().getName() ).renameTo( neo4jHome );
        }
    }

    private void ensureDownloaded( File file, String downloadLink ) throws IOException
    {
        if ( file.exists() )
        {
            file.delete();
        }
        file.getParentFile().mkdirs();
        System.out.println( "Copying: " + downloadLink + " -> " + file );
        streamFileTo( downloadLink, file );

    }

    public void startServer() throws IOException, InterruptedException
    {
        if ( canControlServer() )
        {
            assertFalse( "A server instance is already running", serverResponds() );

            FileTools.deleteRecursively( new File( dataDir, "graph.db" ) );

            Process process = runNeo4j( "start" );
            stopOnExit();

            awaitServerResponds( process );
        }
    }

    public Process runNeo4j( String cmd ) throws IOException
    {
        File startScript = new File( neo4jHome, "bin/neo4j" );
        startScript.setExecutable( true );
        return new ProcessBuilder().inheritIO().command( startScript.getAbsolutePath(), cmd ).start();
    }

    public void stopServer() throws IOException, InterruptedException
    {
        if ( canControlServer() )
        {
            runNeo4j( "stop" ).waitFor();
        }
    }

    public void enableTLS( boolean isTLSEnabled )
    {
        this.isTLSEnabled = isTLSEnabled;
        setServerProperty( "xx.ndp.tls.enabled", String.valueOf( isTLSEnabled ) );
    }

    /**
     * Write the new property and its value in neo4j-server.properties.
     * If the server is already running, then stop and restart the server to reload the changes in the property file
     * @param name
     * @param value
     */
    private void setServerProperty( String name, String value )
    {
        File oldFile = new File( neo4jHome, "conf/neo4j-server.properties" );
        try
        {
            FileTools.setProperty( oldFile, name, value );

            System.out.println( "Restart server to reload property change: " + name + "=" + value );
            this.stopServer();
            this.startServer();
        }
        catch ( Exception e )
        {
            System.out.println( "Failed to change property." );
            throw new RuntimeException( e );
        }
    }

    public boolean canControlServer()
    {
        return !externalServer;
    }

    private void awaitServerResponds( Process process ) throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        for (; ; )
        {
            process.waitFor();
            if ( serverResponds() )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }

            if ( System.currentTimeMillis() > timeout )
            {
                throw new RuntimeException( "Waited for 30 seconds for server to respond to socket calls, " +
                                            "but no response, timing out to avoid blocking forever." );
            }
        }
    }

    private boolean serverResponds() throws IOException, InterruptedException
    {
        try
        {
            URI uri = URI.create( DEFAULT_URL );
            Config config = Config.defaultConfig();
            if( isTLSEnabled )
            {
                config = Config.build().withTLSEnabled( true ).toConfig();
                if( config.knownCerts().exists() )
                {
                    config.knownCerts().delete();
                }
                config.knownCerts().deleteOnExit();
            }
            SocketClient client = new SocketClient( uri.getHost(), uri.getPort(),
                    config, new DevNullLogger() );
            client.start();
            client.stop();
            return true;
        }
        catch ( ClientException e )
        {
            return false;
        }
    }

    /** To allow retrieving a runnable neo4j jar from the international webbernets, we have this */
    private static void streamFileTo( String url, File target ) throws IOException
    {
        try ( FileOutputStream out = new FileOutputStream( target );
              InputStream in = new URL( url ).openStream() )
        {
            byte[] buffer = new byte[1024];
            int read = in.read( buffer );
            while ( read != -1 )
            {
                if ( read > 0 )
                {
                    out.write( buffer, 0, read );
                }

                read = in.read( buffer );
            }
        }
    }

    private void stopOnExit()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    stopServer();
                }
                catch ( Exception e )
                {
                    // cannot help you anything sorry
                    e.printStackTrace();
                }
            }
        } ) );
    }
}
