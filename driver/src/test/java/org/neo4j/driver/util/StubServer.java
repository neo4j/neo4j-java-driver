/*
 * Copyright (c) "Neo4j"
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.neo4j.driver.Config;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.Logging.none;
import static org.neo4j.driver.util.DaemonThreadFactory.daemon;

public class StubServer
{
    private static final int SOCKET_CONNECT_ATTEMPTS = 20;

    public static final Config INSECURE_CONFIG = insecureBuilder().withLogging( none() ).build();

    private static final ExecutorService executor = newCachedThreadPool( daemon( "stub-server-output-reader-" ) );

    // This may be thrown if the driver has not been closed properly
    public static class ForceKilled extends Exception {}

    private static final String BOLT_COMMAND = "bolt";
    private static final String BOLT_STUB_COMMAND = "stub";

    private Process process;

    private StubServer( String script, int port ) throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.addAll( asList( BOLT_COMMAND, BOLT_STUB_COMMAND ) );
        command.addAll( asList( "-l", "localhost:" + port, script ) );
        ProcessBuilder server = new ProcessBuilder().command( command );
        process = server.start();
        startReadingOutput( process );
        waitForSocket( port );
    }

    public static StubServer start( String resource, int port ) throws IOException, InterruptedException
    {
        assumeTrue( boltKitAvailable() );
        return new StubServer( resource(resource), port );
    }

    public int exitStatus() throws InterruptedException
    {
        sleep( 500 );  // wait for a moment to allow disconnection to occur
        try
        {
            return process.exitValue();
        }
        catch ( IllegalThreadStateException ex )
        {
            // not exited yet
            exit();
        }
        return -1;
    }

    public static Config.ConfigBuilder insecureBuilder()
    {
        return Config.builder().withoutEncryption().withLogging( none() );
    }

    private void exit()
    {
        process.destroy();
        try
        {
            process.waitFor();
        }
        catch ( InterruptedException ex )
        {
            throw new RuntimeException( "Interrupted whilst waiting for forced stub shutdown", ex);
        }
    }

    private static String resource( String fileName )
    {
        File resource = new File( DatabaseExtension.TEST_RESOURCE_FOLDER_PATH, fileName );
        if ( !resource.exists() )
        {
            fail( fileName + " does not exists" );
        }
        return resource.getAbsolutePath();
    }

    private static boolean boltKitAvailable()
    {
        try
        {
            // run 'help' command to see if boltstub is available
            Process process = new ProcessBuilder( "bolt" ).start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        }
        catch ( IOException | InterruptedException e )
        {
            // unable to run boltstub command, thus it is unavailable
            return false;
        }
    }

    private static void waitForSocket( int port ) throws InterruptedException
    {
        SocketAddress address = new InetSocketAddress( "localhost", port );
        for ( int i = 0; i < SOCKET_CONNECT_ATTEMPTS; i++ )
        {
            try
            {
                SocketChannel.open( address );
                return;
            }
            catch ( Exception e )
            {
                sleep( 300 );
            }
        }
        throw new AssertionError( "Can't connect to " + address );
    }

    /**
     * Read output of the given process using a separate thread.
     * Since maven-surefire-plugin 2.20.0 it is not good to simply inherit IO using {@link ProcessBuilder#inheritIO()}.
     * It will result in "Corrupted stdin stream in forked JVM 1" warning being printed and output being redirected to a
     * separate temporary file.
     * <p>
     * Fore more details see:
     * <ul>
     * <li>http://maven.apache.org/surefire/maven-surefire-plugin/faq.html#corruptedstream</li>
     * <li>https://issues.apache.org/jira/browse/SUREFIRE-1359</li>
     * </ul>
     *
     * @param process the process to read output.
     */
    private static void startReadingOutput( Process process )
    {
        executor.submit( () ->
        {
            try ( BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) )
            {
                String line;
                while ( (line = reader.readLine()) != null )
                {
                    System.out.println( line );
                }
            }
            return null;
        } );
    }
}
