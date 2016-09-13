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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StubServer
{
    // This may be thrown if the driver has not been closed properly
    public static class ForceKilled extends Exception {}

    private static final int DEFAULT_PORT = 7687;

    private Process process = null;

    private StubServer( String script, int port ) throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.addAll( singletonList( "/usr/local/bin/boltstub" ) );
        command.addAll( asList( Integer.toString( port ), script ) );
        ProcessBuilder server = new ProcessBuilder().inheritIO().command( command );
        process = server.start();
        sleep( 500 );  // might take a moment for the socket to start listening
    }

    public static StubServer start( String script ) throws IOException, InterruptedException
    {
        return start( script, DEFAULT_PORT );
    }

    public static StubServer start( String script, int port ) throws IOException, InterruptedException
    {
        return new StubServer( script, port );
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
            process.destroy();
            process.waitFor();
            throw new ForceKilled();
        }
    }

}
