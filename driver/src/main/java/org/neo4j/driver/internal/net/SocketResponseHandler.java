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
package org.neo4j.driver.internal.net;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.summary.InternalNotification;
import org.neo4j.driver.internal.summary.InternalPlan;
import org.neo4j.driver.internal.summary.InternalProfiledPlan;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.util.Function;

public class SocketResponseHandler implements MessageHandler
{
    private final Queue<Collector> collectors = new ConcurrentLinkedQueue<>();

    /** If a failure occurs, the error gets stored here */
    private Neo4jException error;

    public int collectorsWaiting()
    {
        return collectors.size();
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        Collector collector = collectors.element();
        collector.record( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        Collector collector = collectors.remove();
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
        if ( collector != null )
        {
            collector.doneFailure( error );
        }
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        Collector collector = collectors.remove();
        collectServer( collector, meta.get( "server" ));
        collectFields( collector, meta.get( "fields" ) );
        collectType( collector, meta.get( "type" ) );
        collectStatistics( collector, meta.get( "stats" ) );
        collectPlan( collector, meta.get( "plan" ) );
        collectProfile( collector, meta.get( "profile" ) );
        collectNotifications( collector, meta.get( "notifications" ) );
        collectResultAvailableAfter( collector, meta.get("result_available_after"));
        collectResultConsumedAfter( collector, meta.get("result_consumed_after"));
        collectBookmark( collector, meta.get( "bookmark" ) );
        collector.doneSuccess();
    }

    private void collectServer( Collector collector, Value server )
    {
        if (server != null)
        {
            collector.server( server.asString() );
        }
    }

    private void collectResultAvailableAfter( Collector collector, Value resultAvailableAfter )
    {
        if (resultAvailableAfter != null)
        {
            collector.resultAvailableAfter(resultAvailableAfter.asLong());
        }
    }

    private void collectResultConsumedAfter( Collector collector, Value resultConsumedAfter )
    {
        if (resultConsumedAfter != null)
        {
            collector.resultConsumedAfter(resultConsumedAfter.asLong());
        }
    }

    private void collectNotifications( Collector collector, Value notifications )
    {
        if ( notifications != null )
        {
            Function<Value,Notification> notification = InternalNotification
                    .VALUE_TO_NOTIFICATION;
            collector.notifications( notifications.asList( notification ) );
        }
    }

    private void collectPlan( Collector collector, Value plan )
    {
        if ( plan != null )
        {
            collector.plan( InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( plan ) );
        }
    }

    private void collectProfile( Collector collector, Value plan )
    {
        if ( plan != null )
        {
            collector.profile( InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( plan ) );
        }
    }

    private void collectFields( Collector collector, Value fieldValue )
    {
        if ( fieldValue != null )
        {
            if ( !fieldValue.isEmpty() )
            {
                String[] fields = new String[fieldValue.size()];
                int idx = 0;
                for ( Value value : fieldValue.values() )
                {
                    fields[idx++] = value.asString();
                }
                collector.keys( fields );
            }
        }
    }

    private void collectType( Collector collector, Value type )
    {
        if ( type != null )
        {
            collector.statementType( StatementType.fromCode( type.asString() ) );
        }
    }

    private void collectStatistics( Collector collector, Value stats )
    {
        if ( stats != null )
        {
            collector.statementStatistics(
                    new InternalSummaryCounters(
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

    private void collectBookmark( Collector collector, Value bookmark )
    {
        if ( bookmark != null )
        {
            collector.bookmark( bookmark.asString() );
        }
    }

    private int statsValue( Value stats, String name )
    {
        Value value = stats.get( name );
        return value.isNull() ? 0 : value.asInt();
    }

    @Override
    public void handleIgnoredMessage()
    {
        Collector collector = collectors.remove();
        if (collector != null)
        {
            collector.doneIgnored();
        }
    }

    @Override
    public void handleDiscardAllMessage()
    {

    }

    @Override
    public void handleResetMessage()
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
    public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken )
    {

    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {

    }

    public void appendResultCollector( Collector collector )
    {
        assert collector != null;

        collectors.add( collector );
    }

    public boolean protocolViolationErrorOccurred()
    {
        return error != null && error.code().startsWith( "Neo.ClientError.Request" );
    }

    public boolean serverFailureOccurred()
    {
        return error != null;
    }

    public Neo4jException serverFailure()
    {
        return error;
    }

    public void clearError()
    {
        error = null;
    }
}
