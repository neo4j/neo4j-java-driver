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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.neo4j.driver.Config;

import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.util.DaemonThreadFactory.daemon;

public class StubServer
{
    private static final String TEST_RESOURCE_FOLDER_PATH = "src/test/resources";

    private static final int SOCKET_CONNECT_ATTEMPTS = 20;

    public static final Config INSECURE_CONFIG = Config.builder().withoutEncryption().build();

    private static final ExecutorService executor = newCachedThreadPool( daemon( "stub-server-output-reader-" ) );

    // This may be thrown if the driver has not been closed properly
    public static class ForceKilled extends Exception {}

    private static final String BOLT_STUB_COMMAND_FORMAT = "bolt stub -l localhost:%s -t 5 %s";

    private final Process process;

    private StubServer( String script, int port ) throws IOException, InterruptedException
    {
        String str = String.format( BOLT_STUB_COMMAND_FORMAT, port, script );
        List<String> command = Arrays.asList( str.split( "\\s+" ) );
        ProcessBuilder server = new ProcessBuilder().command( command ).inheritIO();
        process = server.start();
        waitForSocket( port );
    }

    public static StubServer start( String resource, int port ) throws IOException, InterruptedException
    {
        assumeTrue( boltKitAvailable() );
        return new StubServer( resource(resource), port );
    }

    public int exitStatus() throws InterruptedException, ForceKilled
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
            throw new ForceKilled();
        }
    }

    private void exit() throws InterruptedException
    {
        process.destroy();
        process.waitFor();
    }

    private static String resource( String fileName )
    {
        File resource = new File( TEST_RESOURCE_FOLDER_PATH, fileName );
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
            // run 'help' command to see if bolt stub is available
            Process process = new ProcessBuilder( "bolt stub --help".split( "\\s+" ) ).start();
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
}
