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
package org.neo4j.driver.v1.internal.connector.socket;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.internal.summary.SimpleNotification;
import org.neo4j.driver.v1.internal.summary.SimplePlan;
import org.neo4j.driver.v1.internal.summary.SimpleProfiledPlan;
import org.neo4j.driver.v1.internal.summary.SimpleUpdateStatistics;
import org.neo4j.driver.v1.internal.messaging.MessageHandler;
import org.neo4j.driver.v1.internal.spi.StreamCollector;

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
            switch ( classification )
            {
                case "ClientError":
                    error = new ClientException( code, message );
                    break;
                case "TransientError":
                    error = new TransientException( code, message );
                    break;
                default:
                    error = new DatabaseException( code, message );
                    break;
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
            collectFields( collector, meta.get( "fields" ) );
            collectType( collector, meta.get( "type" ) );
            collectStatistics( collector, meta.get( "stats" ) );
            collectPlan( collector, meta.get( "plan" ) );
            collectProfile( collector, meta.get( "profile" ) );
            collectNotifications( collector, meta.get( "notifications" ) );
        }
        responseId++;
    }

    private void collectNotifications( StreamCollector collector, Value notifications )
    {
        if ( notifications != null )
        {
            collector.notifications( notifications.javaList( SimpleNotification.VALUE_TO_NOTIFICATION ) );
        }
    }

    private void collectPlan( StreamCollector collector, Value plan )
    {
        if ( plan != null )
        {
            collector.plan( SimplePlan.EXPLAIN_PLAN_FROM_VALUE.apply( plan ) );
        }
    }

    private void collectProfile( StreamCollector collector, Value plan )
    {
        if ( plan != null )
        {
            collector.profile( SimpleProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( plan ) );
        }
    }

    private void collectFields( StreamCollector collector, Value fieldValue )
    {
        if (fieldValue != null)
        {
            if ( fieldValue.size() > 0 )
            {
                String[] fields = new String[(int) fieldValue.size()];
                int idx = 0;
                for ( Value value : fieldValue )
                {
                    fields[idx++] = value.javaString();
                }
                collector.fieldNames( fields );
            }
        }
    }

    private void collectType( StreamCollector collector, Value type )
    {
        if ( type != null )
        {
            collector.statementType( StatementType.fromCode( type.javaString() ) );
        }
    }

    private void collectStatistics( StreamCollector collector, Value stats )
    {
        if ( stats != null )
        {
            collector.statementStatistics(
                new SimpleUpdateStatistics(
                    statsValue( stats, "nodes-created" ),
                    statsValue( stats, "nodes-deleted" ),
                    statsValue( stats, "relationships-created" ),
                    statsValue( stats, "relationships-deleted" ),
                    statsValue( stats, "properties-set" ),
                    statsValue( stats, "labels-added" ),
                    statsValue( stats, "labels-removed" ),
                    statsValue( stats, "indexes-added" ),
                    statsValue( stats, "indexes-removed" ),
                    statsValue( stats, "constraints-added" ),
                    statsValue( stats, "constraints-removed" )
                )
            );
        }
    }

    private int statsValue( Value stats, String name )
    {
        Value value = stats.get( name );
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
