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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.InternalBookmark;

public class CommandProcessor
{
    private final Map<String,Driver> drivers = new HashMap<>();
    private final Map<String,Session> sessionStates = new HashMap<>();
    private final Map<String,Result> results = new HashMap<>();
    private final Map<String,Transaction> transactions = new HashMap<>();
    private final Map<String,Exception> errors = new HashMap<>();

    private int idGenerator = 0;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void processRequest( String request, BufferedReader in, BufferedWriter out ) throws IOException
    {
        System.out.println( "request = " + request + ", in = " + in + ", out = " + out );

        JsonNode jsonRequest = objectMapper.readTree( request );

        String requestType = jsonRequest.get( "name" ).asText();

        if (requestType.equals( "NewDriver" ))
        {
            String id = newId();
            String response = "{ \"name\" : \"Driver\", \"data\": {\"id\": \"" + id + "\"} }";
            drivers.putIfAbsent( id, GraphDatabase.driver( jsonRequest.get( "data" ).get( "uri" ).asText(), AuthTokens.basic( "neo4j", "pass" ) ) );
            writeResponse( response, out );
        } else if ( requestType.equals( "NewSession" ))
        {
            String id = jsonRequest.get( "data" ).get( "driverId" ).asText();
            Driver driver = drivers.get( id );
            AccessMode accessMode = jsonRequest.get( "data" ).get( "accessMode" ).asText().equals( "r" ) ? AccessMode.READ : AccessMode.WRITE;
            //Bookmark bookmark = InternalBookmark.parse(  jsonRequest.get( "data" ).get("bookmarks").asText() );
            Session session = driver.session( SessionConfig.builder()
                                         .withDefaultAccessMode( accessMode ).build());
            String newId = newId();
            sessionStates.put( newId, session );
            String response = "{ \"name\" : \"Session\", \"data\": {\"id\": \"" + newId + "\"} }";

            writeResponse( response, out );
        } else if ( requestType.equals( "SessionRun" ))
        {
            String id = jsonRequest.get( "data" ).get( "sessionId" ).asText();
            Session session = sessionStates.get( id );
            Result result = session.run( jsonRequest.get( "data" ).get("cypher").asText() );
            String newId = newId();

            results.put( newId, result );
            String response = "{ \"name\" : \"Result\", \"data\": {\"id\": \"" + newId + "\"} }";

            writeResponse( response, out );
        } else if ( requestType.equals( "ResultNext" ))
        {
            String id = jsonRequest.get( "data" ).get( "resultId" ).asText();
            Result result = results.get( id );
            Record record = result.next();
            String newId = newId();

            results.put( newId, result );
            String response = "{ \"name\" : \"Record\", \"data\": {\"values\" : [" + NutkitTypes.fromRecord( record ) + "] } }";

            writeResponse( response, out );
        } else if ( requestType.equals( "SessionClose" ))
        {
            String id = jsonRequest.get( "data" ).get( "sessionId" ).asText();
            Session session = sessionStates.get( id );
            session.close();
            sessionStates.remove( id );

            String response = "{ \"name\" : \"Session\", \"data\": {\"id\": \"" + id + "\"} }";

            writeResponse( response, out );
        }
        else if ( requestType.equals( "DriverClose" ))
        {
            String id = jsonRequest.get( "data" ).get( "driverId" ).asText();
            Driver driver = drivers.get( id );
            driver.close();
            drivers.remove( id );

            String response = "{ \"name\" : \"Driver\", \"data\": {\"id\": \"" + id + "\"} }";

            writeResponse( response, out );
        }

    }

    private void writeResponse(String response, BufferedWriter out) throws IOException
    {
        System.out.println( "responseToNutKit = " + response );

        out.write( "#response begin\n" );
        out.write( response + "\n" );
        out.write( "#response end\n" );
        out.flush();
    }

    private String newId()
    {
        int nextNumber = idGenerator++;
        System.out.println(nextNumber);
        return String.valueOf( nextNumber );
    }
}
