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
import java.io.UncheckedIOException;
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
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalBookmark;

public class CommandProcessor
{
    private final Map<String, Driver> drivers = new HashMap<>();
    private final Map<String, SessionState> sessionStates = new HashMap<>();
    private final Map<String, Result> results = new HashMap<>();
    private final Map<String, Transaction> transactions = new HashMap<>();
    private final Map<String, Neo4jException> errors = new HashMap<>();

    private int idGenerator = 0;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final BufferedReader in;
    private final BufferedWriter out;

    public CommandProcessor(BufferedReader in, BufferedWriter out) {
        this.in = in;
        this.out = out;
    }

    private String readLine() {
        try {
            return this.in.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void write(String s) {
        try {
            this.out.write(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Logs to frontend
    private void log(String s) {
        try {
            this.out.write(s + "\n");
            this.out.flush();
        } catch (IOException e) { }
        System.out.println(s);
    }

    private void flush() {
        try {
            this.out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Reads one request and writes the response. Returns false when not able to read anymore.
    public boolean process() {
        boolean inRequest = false;
        StringBuilder request = new StringBuilder();

        log("Waiting for request");

        while (true) {
            String currentLine = readLine();
            // End of stream
            if ( currentLine == null) {
                return false;
            }

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
                    processRequest( request.toString());
                } catch ( Exception e ){
                    if (e instanceof ClientException) {
                        // Client error, no need to track
                        writeResponse(Testkit.wrap("ClientError", Testkit.msg(e.toString())));
                    } else if (e instanceof Neo4jException) {
                        // Error to track
                        String id = newId();
                        errors.put(id, (Neo4jException)e);
                        writeResponse(Testkit.wrap("DriverError", Testkit.id(id)));
                        System.out.println("Neo4jException: " + e);
                    } else {
                        // Unknown error, interpret this as a backend error.
                        // Report to frontend and rethrow, note that if socket been
                        // closed the writing will throw itself...
                        writeResponse(Testkit.wrap("BackendError", Testkit.msg(e.toString())));
                        // This won't print if there was an IO exception since line above will rethrow
                        e.printStackTrace();
                        throw e;
                    }
                }
                return true;
            } else
            {
                if ( !inRequest )
                {
                    throw new RuntimeException( "Command Received whilst not in request");
                }
                request.append( currentLine );
            }
        }
    }

    public void processRequest( String request) 
    {
        System.out.println( "request = " + request + ", in = " + in + ", out = " + out );

        JsonNode jsonRequest;
        try {
            jsonRequest = objectMapper.readTree( request );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String requestType = jsonRequest.get( "name" ).asText();
        JsonNode requestData = jsonRequest.get("data");
        log("Received request: " + requestType);

        if (requestType.equals( "NewDriver" ))
        {
            String id = newId();
            String response = Testkit.wrap("Driver", Testkit.id(id));
            drivers.putIfAbsent( id, GraphDatabase.driver( requestData.get( "uri" ).asText(), AuthTokens.basic( "neo4j", "pass" ) ) );
            writeResponse( response);
        } else if ( requestType.equals( "NewSession" ))
        {
            String id = requestData.get( "driverId" ).asText();
            Driver driver = drivers.get( id );
            AccessMode accessMode = requestData.get( "accessMode" ).asText().equals( "r" ) ? AccessMode.READ : AccessMode.WRITE;
            //Bookmark bookmark = InternalBookmark.parse(  requestData.get("bookmarks").asText() );
            Session session = driver.session( SessionConfig.builder()
                                         .withDefaultAccessMode( accessMode ).build());
            String newId = newId();
            sessionStates.put( newId, new SessionState(session) );
            String response = Testkit.wrap("Session", Testkit.id(newId));

            writeResponse( response);
        } else if ( requestType.equals( "SessionRun" ))
        {
            String id = requestData.get( "sessionId" ).asText();
            Session session = sessionStates.get( id ).session;
            Result result = session.run( requestData.get("cypher").asText() );
            String newId = newId();

            results.put( newId, result );
            String response = Testkit.wrap("Result", Testkit.id(newId));
            writeResponse( response);
        } else if ( requestType.equals( "TransactionRun" ))
        {
            String txId = requestData.get("txId").asText();
            String cypher = requestData.get("cypher").asText();
            Transaction tx = transactions.get(txId);
            Result result = tx.run(cypher);
            String newId = newId();
            results.put(newId, result);
            writeResponse(Testkit.wrap("Result", Testkit.id(newId)));
        } else if ( requestType.equals( "RetryablePositive" ))
        {
            String id = requestData.get( "sessionId" ).asText();
            SessionState sessState = sessionStates.get( id );
            sessState.retryableState = 1;
        } else if ( requestType.equals( "RetryableNegative" ))
        {
            String id = requestData.get( "sessionId" ).asText();
            SessionState sessState = sessionStates.get( id );
            sessState.retryableState = -1;
            sessState.retryableErrorId = requestData.get("errorId").asText();
        } else if ( requestType.equals( "SessionReadTransaction" ))
        {
            String id = requestData.get( "sessionId" ).asText();
            SessionState sessState = sessionStates.get( id );
            sessState.session.readTransaction((Transaction tx) -> {
                // Reset session state
                sessState.retryableState = 0;
                // Stash this transaction as there will be commands using it
                String txId = newId();
                transactions.put(txId, tx);
                // Instruct testkit client to send it's commands within the transaction
                try {
                    writeResponse(Testkit.wrap("RetryableTry", Testkit.id(txId)));
                } catch ( Exception e) {
                    e.printStackTrace();
                }
                while ( true ) {
                    // Process commands as usual but blocking in here
                    process();
                    // Check if state changed on session
                    switch (sessState.retryableState) {
                    case 0:
                        // Nothing happened to session state while processing command
                        break;
                    case 1:
                        // Client is happy to commit
                        return 0;
                    case -1:
                        // Client wants to rollback
                        if (sessState.retryableErrorId != "") {
                            Neo4jException err = errors.get(sessState.retryableErrorId);
                            throw err;
                        } else {
                            throw new RuntimeException("Error from client in retryable tx");
                        }
                    }
                }
            });
            // Assume that exception is thrown by readTransaction when retry fails so this is
            // never reached.
            writeResponse(Testkit.wrap("RetryableDone", "{}"));
        } else if ( requestType.equals( "ResultNext" ))
        {
            String id = requestData.get( "resultId" ).asText();
            Result result = results.get( id );
            Record record = result.next();
            String newId = newId();

            results.put( newId, result );
            String response = Testkit.wrap("Record", Testkit.values(TestkitTypes.fromRecord(record)));

            writeResponse( response);
        } else if ( requestType.equals( "SessionClose" ))
        {
            String id = requestData.get( "sessionId" ).asText();
            Session session = sessionStates.get( id ).session;
            session.close();
            sessionStates.remove( id );

            String response = Testkit.wrap("Session", Testkit.id(id));

            writeResponse( response);
        }
        else if ( requestType.equals( "DriverClose" ))
        {
            String id = requestData.get( "driverId" ).asText();
            Driver driver = drivers.get( id );
            driver.close();
            drivers.remove( id );

            String response = Testkit.wrap("Driver", Testkit.id(id));

            writeResponse( response);
        }
        else
        {
            throw new RuntimeException("Request " + requestType + " not handled");
        }
    }

    private void writeResponse(String response)
    {
        System.out.println( "response = " + response );

        write( "#response begin\n" );
        write( response + "\n" );
        write( "#response end\n" );
        flush();
    }

    private String newId()
    {
        int nextNumber = idGenerator++;
        return String.valueOf( nextNumber );
    }

    private SessionState getSessionState(JsonNode req) {
        String id = req.get( "data" ).get( "sessionId" ).asText();
        return sessionStates.get( id );
    }
}
