/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.connector.socket;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.StatementType;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.StandardResultSummary;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.StreamCollector;

import static org.neo4j.driver.Values.valueToString;

public class SocketResponseHandler implements MessageHandler
{
    private final Map<Integer,StreamCollector> collectors = new HashMap<>();

    /** If a failure occurs, the error gets stored here */
    private Neo4jException error;

    /** Counts number of responses, used to correlate response data with stream collectors */
    private int responseId = 0;

    public int receivedResponses()
    {
        return responseId;
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        // This is not very efficient, using something like a singly-linked queue with a current collector as head
        // would take advantage of the ordered nature of exchanges and avoid all these objects allocated from boxing
        // below.
        StreamCollector collector = collectors.get( responseId );
        if ( collector != null )
        {
            collector.record( fields );
        }
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        try
        {
            String[] parts = code.split( "\\." );
            String classification = parts[1];
            if ( classification.equals( "ClientError" ) )
            {
                error = new ClientException( code, message );
            }
            else if ( classification.equals( "TransientError" ) )
            {
                error = new TransientException( code, message );
            }
            else
            {
                error = new DatabaseException( code, message );
            }
        }
        finally
        {
            responseId++;
        }
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        StreamCollector collector = collectors.get( responseId );
        if ( collector != null )
        {
            if( meta.containsKey( "fields" ) )
            {
                collector.head( meta.get( "fields" ).javaList( valueToString() ) );
            }
            if( meta.containsKey( "type" ))
            {
                System.out.println("EOS: " + meta );
                Value stats = meta.get( "stats" );
                collector.tail( new StandardResultSummary(
                    intOrZero( stats, "nodes-created" ),
                    intOrZero( stats, "nodes-deleted" ),
                    intOrZero( stats, "relationships-created" ),
                    intOrZero( stats, "relationships-deleted" ),
                    intOrZero( stats, "properties-set" ),
                    intOrZero( stats, "labels-added" ),
                    intOrZero( stats, "labels-removed" ),
                    intOrZero( stats, "indexes-added" ),
                    intOrZero( stats, "indexes-removed" ),
                    intOrZero( stats, "constraints-added" ),
                    intOrZero( stats, "constraints-removed" ),
                    statementType( meta.get( "type" ) )
                ));
            }
        }
        responseId++;
    }

    private StatementType statementType( Value type )
    {
        switch( type.javaString() )
        {
        case "r":  return StatementType.READ_ONLY;
        case "rw": return StatementType.READ_WRITE;
        case "w":  return StatementType.WRITE_ONLY;
        case "s":  return StatementType.SCHEMA_WRITE;
        default: throw new ClientException( "Unknown statement type code: `" + type.javaBoolean() + "`." );
        }
    }

    private int intOrZero( Value stats, String key )
    {
        if( stats == null )
        {
            return 0;
        }
        Value value = stats.get( key );
        return value == null ? 0 : value.javaInteger();
    }

    @Override
    public void handleIgnoredMessage()
    {
        responseId++;
    }

    @Override
    public void handleDiscardAllMessage()
    {

    }

    @Override
    public void handleAckFailureMessage()
    {

    }

    @Override
    public void handlePullAllMessage()
    {

    }

    @Override
    public void handleInitMessage( String clientNameAndVersion )
    {

    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {

    }

    public void registerResultCollector( int correlationId, StreamCollector collector )
    {
        collectors.put( correlationId, collector );
    }

    public boolean serverFailureOccurred()
    {
        return error != null;
    }

    public Neo4jException serverFailure()
    {
        return error;
    }

    public void clear()
    {
        responseId = 0;
        error = null;
        collectors.clear();
    }
}