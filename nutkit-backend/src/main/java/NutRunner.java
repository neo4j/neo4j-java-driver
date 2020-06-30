/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class NutRunner
{

    public static void main( String[] args ) throws IOException
    {
        ServerSocket serverSocket = new ServerSocket(9876);

        System.out.println("Starting Java Nut Backend Started");

        Socket clientSocket = serverSocket.accept();
        System.out.println( "Handling connection from: " + clientSocket.getRemoteSocketAddress() );

        BufferedReader in = new BufferedReader( new InputStreamReader( clientSocket.getInputStream() ) );
        BufferedWriter out = new BufferedWriter( new OutputStreamWriter( clientSocket.getOutputStream() ) );

        boolean inRequest = false;
        StringBuilder request = new StringBuilder();
        String currentLine = "";
        CommandProcessor commandProcessor = new CommandProcessor();

        while ((currentLine = in.readLine()) != null)
        {
            System.out.println(currentLine);
            if ( currentLine.equals( "#request begin" ))
            {
                inRequest = true;
            } else if (currentLine.equals( "#request end" ))
            {
                if ( !inRequest )
                {
                    throw new RuntimeException( "Request end not expected" );
                }
                try
                {
                    commandProcessor.processRequest( request.toString(), in, out);
                } catch ( Exception e ){
                    out.write( e.toString() + "\n" );
                    out.flush();
                    e.printStackTrace();
                    throw new RuntimeException("Error processing request" + e);
                }
                request = new StringBuilder();
                inRequest = false;
            } else
            {
                if ( !inRequest )
                {
                    throw new RuntimeException( "Command Received whilst not in request");
                }
                request.append( currentLine );
            }
        }

        clientSocket.close();
        serverSocket.close();
        System.out.println("Java Nut Backend Terminated");
    }
}
