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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.BackendError;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;

class DefaultCommandProcessor implements CommandProcessor
{
    private final TestkitState testkitState;

    private final ObjectMapper objectMapper;

    private final BufferedReader in;
    private final BufferedWriter out;

    DefaultCommandProcessor( BufferedReader in, BufferedWriter out )
    {
        this.in = in;
        this.out = out;
        this.objectMapper = CommandProcessor.newObjectMapperFor( this );
        this.testkitState = new TestkitState( this::writeResponse );
    }

    private String readLine()
    {
        try
        {
            return this.in.readLine();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void write( String s )
    {
        try
        {
            this.out.write( s );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    // Logs to frontend
    private void log( String s )
    {
        try
        {
            this.out.write( s + "\n" );
            this.out.flush();
        }
        catch ( IOException e )
        {
        }
        System.out.println( s );
    }

    private void flush()
    {
        try
        {
            this.out.flush();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public boolean process()
    {
        boolean inRequest = false;
        StringBuilder request = new StringBuilder();

        log( "Waiting for request" );

        while ( true )
        {
            String currentLine = readLine();
            // End of stream
            if ( currentLine == null )
            {
                return false;
            }

            if ( currentLine.equals( "#request begin" ) )
            {
                inRequest = true;
            }
            else if ( currentLine.equals( "#request end" ) )
            {
                if ( !inRequest )
                {
                    throw new RuntimeException( "Request end not expected" );
                }
                try
                {
                    processRequest( request.toString() );
                }
                catch ( Exception e )
                {
                    if ( e instanceof Neo4jException )
                    {
                        // Error to track
                        String id = testkitState.newId();
                        testkitState.getErrors().put( id, (Neo4jException) e );
                        writeResponse( driverError( id, (Neo4jException) e ) );
                        System.out.println( "Neo4jException: " + e );
                    }
                    else if ( isConnectionPoolClosedException( e ) || e instanceof UntrustedServerException )
                    {
                        String id = testkitState.newId();
                        DriverError driverError = DriverError.builder()
                                                             .data(
                                                                     DriverError.DriverErrorBody.builder()
                                                                                                .id( id )
                                                                                                .errorType( e.getClass().getName() )
                                                                                                .msg( e.getMessage() )
                                                                                                .build()
                                                             )
                                                             .build();
                        writeResponse( driverError );
                    }
                    else
                    {
                        // Unknown error, interpret this as a backend error.
                        // Report to frontend and rethrow, note that if socket been
                        // closed the writing will throw itself...
                        writeResponse( BackendError.builder().data( BackendError.BackendErrorBody.builder().msg( e.toString() ).build() ).build() );
                        // This won't print if there was an IO exception since line above will rethrow
                        e.printStackTrace();
                        throw e;
                    }
                }
                return true;
            }
            else
            {
                if ( !inRequest )
                {
                    throw new RuntimeException( "Command Received whilst not in request" );
                }
                request.append( currentLine );
            }
        }
    }

    private DriverError driverError( String id, Neo4jException e )
    {
        return DriverError.builder().data(
                DriverError.DriverErrorBody.builder()
                                           .id( id )
                                           .errorType( e.getClass().getName() )
                                           .code( e.code() )
                                           .msg( e.getMessage() )
                                           .build() )
                          .build();
    }

    public void processRequest( String request )
    {
        System.out.println( "request = " + request + ", in = " + in + ", out = " + out );
        try
        {
            TestkitRequest testkitMessage = objectMapper.readValue( request, TestkitRequest.class );
            TestkitResponse response = testkitMessage.process( testkitState );
            if ( response != null )
            {
                writeResponse( response );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeResponse( TestkitResponse response )
    {
        try
        {
            String responseStr = objectMapper.writeValueAsString( response );
            System.out.println("response = " + responseStr + ", in = " + in + ", out = " + out);
            write( "#response begin\n" );
            write( responseStr + "\n" );
            write( "#response end\n" );
            flush();
        }
        catch ( JsonProcessingException ex )
        {
            throw new RuntimeException( "Error writing response", ex );
        }
    }

    private boolean isConnectionPoolClosedException( Exception e )
    {
        return e instanceof IllegalStateException && e.getMessage() != null &&
               e.getMessage().equals( ConnectionPoolImpl.CONNECTION_POOL_CLOSED_ERROR_MESSAGE );
    }
}
