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
package org.neo4j.driver.util.cc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.driver.util.DaemonThreadFactory;
import org.neo4j.driver.util.ProcessEnvConfigurator;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CommandLineUtil
{
    private static final ExecutorService executor = Executors.newCachedThreadPool(
            new DaemonThreadFactory( "command-line-thread-" ) );

    public static boolean boltKitAvailable()
    {
        try
        {
            executeCommand( "neoctrl-cluster", "--help" );
            return true;
        }
        catch ( CommandLineException e )
        {
            return false;
        }
    }

    public static String executeCommand( List<String> commands )
    {
        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder().command( commands );
            ProcessEnvConfigurator.configure( processBuilder );
            return executeAndGetStdOut( processBuilder );
        }
        catch ( IOException | CommandLineException e )
        {
            throw new CommandLineException( "Error running command " + commands, e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new CommandLineException( "Interrupted while waiting for command " + commands, e );
        }
    }

    public static String executeCommand( String... command )
    {
        return executeCommand( asList( command ) );
    }

    private static String executeAndGetStdOut( ProcessBuilder processBuilder )
            throws IOException, InterruptedException
    {
        Process process = processBuilder.start();
        Future<String> stdOutFuture = read( process.getInputStream() );
        Future<String> stdErrFuture = read( process.getErrorStream() );
        int exitCode = process.waitFor();
        String stdOut = get( stdOutFuture );
        String stdErr = get( stdErrFuture );
        if ( exitCode != 0 )
        {
            throw new CommandLineException( "Non-zero exit code\nSTDOUT:\n" + stdOut + "\nSTDERR:\n" + stdErr );
        }
        return stdOut;
    }

    private static Future<String> read( final InputStream input )
    {
        return executor.submit( new Callable<String>()
        {
            @Override
            public String call() throws Exception
            {
                return readToString( input );
            }
        } );
    }

    private static String readToString( InputStream input )
    {
        StringBuilder result = new StringBuilder();
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( input ) ) )
        {
            String line;
            while ( (line = reader.readLine()) != null )
            {
                result.append( line ).append( lineSeparator() );
            }
        }
        catch ( IOException e )
        {
            throw new CommandLineException( "Unable to read from stream", e );
        }
        return result.toString();
    }

    private static <T> T get( Future<T> future )
    {
        try
        {
            return future.get( 10, MINUTES );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
