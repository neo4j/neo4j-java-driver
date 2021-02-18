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
package neo4j.org.testkit.backend;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

public class Runner
{
    public static void main( String[] args ) throws IOException
    {
        ServerSocket serverSocket = new ServerSocket( 9876 );

        System.out.println( "Java TestKit Backend Started on port: " + serverSocket.getLocalPort() );

        while ( true )
        {
            final Socket clientSocket = serverSocket.accept();
            CompletableFuture.runAsync( () -> handleClient( clientSocket ) );
        }
    }

    private static void handleClient( Socket clientSocket )
    {
        try
        {
            System.out.println( "Handling connection from: " + clientSocket.getRemoteSocketAddress() );
            BufferedReader in = new BufferedReader( new InputStreamReader( clientSocket.getInputStream() ) );
            BufferedWriter out = new BufferedWriter( new OutputStreamWriter( clientSocket.getOutputStream() ) );
            CommandProcessor commandProcessor = new CommandProcessor( in, out );

            boolean cont = true;
            while ( cont )
            {
                try
                {
                    cont = commandProcessor.process();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    clientSocket.close();
                    cont = false;
                }
            }
        }
        catch ( IOException ex )
        {
            throw new UncheckedIOException( ex );
        }
    }
}
