/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.util;

import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;

import org.neo4j.Neo4j;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.connector.socket.SocketClient;

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

    private final String neo4jVersion = System.getProperty( "version", "neo4j-community-2.3.0-M01" );
    private final String neo4jLink = System.getProperty( "packageUri",
            "http://dist.neo4j.org/" + neo4jVersion + "-unix.tar.gz" );
    private final String remotingExtensionLink =
            "http://m2.neo4j.org/service/local/artifact/maven/content?r=snapshots&g=org.neo4j" +
            ".ndp&a=neo4j-ndp-kernelextension&v=LATEST";

    private final File neo4jDir = new File( "./target/neo4j" );
    private final File neo4jHome = new File( neo4jDir, neo4jVersion );
    private final File dataDir = new File( neo4jHome, "data" );

    private final File neo4jJar = new File( "./target/" + neo4jVersion + ".tar.gz" );
    private final File remotingExtensionJar = new File( neo4jHome, "plugins/remoting.jar" );


    public static void main( String... args ) throws Exception
    {
        Neo4jRunner neo4jRunner = new Neo4jRunner();
        neo4jRunner.startServer();
        neo4jRunner.stopServer();
    }

    /** Globall runner controlling a single server, used to avoid having to restart the server between tests */
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
            if ( !neo4jHome.exists() || neo4jHome.list() == null || !remotingExtensionJar.exists() ||
                 remotingExtensionJar.length() == 0 )
            {
                // no neo4j exists or no files inside the folder

                // download neo4j server from a URL
                ensureFileExist( neo4jJar, neo4jLink );

                // Untar the neo4j server
                System.out.println( "Untarring: " + neo4jJar + " -> " + neo4jDir );
                Archiver archiver = ArchiverFactory.createArchiver( "tar", "gz" );
                archiver.extract( neo4jJar, neo4jDir );

                // put the ndp extension into the 'plugins' directory
                ensureFileExist( remotingExtensionJar, remotingExtensionLink );

                // Add experimental.ndp.enabled=true to conf/neo4j.properties
                File configFile = new File( neo4jHome, "conf/neo4j.properties" );
                System.out.println( "Enabling ndp in " + configFile );
                try ( PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( configFile, true ) ) ) )
                {
                    out.println( "experimental.ndp.enabled=true" );
                }
                catch ( IOException e )
                {
                    throw e;
                }
            }
            else
            {
                System.out.println( "Using Neo4j server in: " + neo4jHome.getAbsolutePath() );
            }
        }
    }

    private void ensureFileExist( File jarFile, String downloadLink ) throws IOException
    {
        if ( jarFile.exists() && jarFile.length() == 0 )
        {
            jarFile.delete();
        }
        if ( !jarFile.exists() )
        {
            jarFile.getParentFile().mkdirs();
            System.out.println( "Copying: " + downloadLink + " -> " + jarFile );
            streamFileTo( downloadLink, jarFile );
        }
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

    public void clearData()
    {
        // Note - this hangs for extended periods some times, because there are tests that leave sessions running.
        // Thus, we need to wait for open sessions and transactions to time out before this will go through.
        // This could be helped by an extension in the future.
        try ( Session session = Neo4j.session( address() ) )
        {
            session.run( "MATCH (n) OPTIONAL MATCH (n)-[r]->() DELETE r,n" );
        }
    }

    public void stopServer() throws IOException, InterruptedException
    {
        if ( canControlServer() )
        {
            runNeo4j( "stop" ).waitFor();
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
            SocketClient client = new SocketClient( uri.getHost(), uri.getPort() );
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

    public String address()
    {
        return DEFAULT_URL;
    }

}
