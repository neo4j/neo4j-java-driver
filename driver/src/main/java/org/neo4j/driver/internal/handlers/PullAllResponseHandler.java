/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.handlers;

import io.netty.util.concurrent.Promise;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.netty.AsyncConnection;
import org.neo4j.driver.internal.netty.InternalTask;
import org.neo4j.driver.internal.netty.Task;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.summary.InternalNotification;
import org.neo4j.driver.internal.summary.InternalPlan;
import org.neo4j.driver.internal.summary.InternalProfiledPlan;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;

public class PullAllResponseHandler implements ResponseHandler
{
    private static final boolean TOUCH_AUTO_READ = false;

    private final RunMetadataAccessor runMetadataAccessor;
    private final AsyncConnection connection;
    private final boolean releaseConnection;

    private final Queue<Record> records;
    private Promise<Boolean> recordAvailablePromise;
    private boolean succeeded;
    private Throwable failure;

    // todo: expose result summary
    private StatementType statementType;
    private SummaryCounters counters;
    private Plan plan;
    private ProfiledPlan profile;
    private List<Notification> notifications;
    private long resultConsumedAfter;

    private volatile Record current;

    public PullAllResponseHandler( RunMetadataAccessor runMetadataAccessor, AsyncConnection connection,
            boolean releaseConnection )
    {
        this.runMetadataAccessor = runMetadataAccessor;
        this.connection = connection;
        this.releaseConnection = releaseConnection;
        this.records = new LinkedList<>();
    }

    @Override
    public synchronized void onSuccess( Map<String,Value> metadata )
    {
        statementType = extractStatementType( metadata );
        counters = extractCounters( metadata );
        plan = extractPlan( metadata );
        profile = extractProfiledPlan( metadata );
        notifications = extractNotifications( metadata );
        resultConsumedAfter = extractResultConsumedAfter( metadata );

        succeeded = true;
        releaseConnectionIfNeeded();

        if ( recordAvailablePromise != null )
        {
            recordAvailablePromise.setSuccess( false );
        }
    }

    @Override
    public synchronized void onFailure( Throwable error )
    {
        failure = error;
        releaseConnectionIfNeeded();

        if ( recordAvailablePromise != null )
        {
            recordAvailablePromise.setFailure( error );
            recordAvailablePromise = null;
        }
    }

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        Record record = new InternalRecord( runMetadataAccessor.statementKeys(), fields );

        if ( recordAvailablePromise != null )
        {
            current = record;
            recordAvailablePromise.setSuccess( true );
            recordAvailablePromise = null;
        }
        else
        {
            queueRecord( record );
        }
    }

    public synchronized Task<Boolean> recordAvailable()
    {
        Record record = dequeueRecord();
        if ( record == null )
        {
            if ( succeeded )
            {
                Promise<Boolean> result = connection.newPromise();
                result.setSuccess( false );
                return new InternalTask<>( result );
            }

            if ( failure != null )
            {
                Promise<Boolean> result = connection.newPromise();
                result.setFailure( failure );
                return new InternalTask<>( result );
            }

            recordAvailablePromise = connection.newPromise();
            return new InternalTask<>( recordAvailablePromise );
        }
        else
        {
            current = record;

            Promise<Boolean> result = connection.newPromise();
            result.setSuccess( true );
            return new InternalTask<>( result );
        }
    }

    public Record pollCurrent()
    {
        Record result = current;
        current = null;
        return result;
    }

    private void queueRecord( Record record )
    {
        records.add( record );
        if ( TOUCH_AUTO_READ )
        {
            if ( records.size() > 10_000 )
            {
                connection.disableAutoRead();
            }
        }
    }

    private Record dequeueRecord()
    {
        Record record = records.poll();
        if ( TOUCH_AUTO_READ )
        {
            if ( record != null && records.size() < 100 )
            {
                connection.enableAutoRead();
            }
        }
        return record;
    }

    private void releaseConnectionIfNeeded()
    {
        if ( releaseConnection )
        {
            connection.release();
        }
    }

    private static StatementType extractStatementType( Map<String,Value> metadata )
    {
        Value typeValue = metadata.get( "type" );
        if ( typeValue != null )
        {
            return StatementType.fromCode( typeValue.asString() );
        }
        return null;
    }

    private static InternalSummaryCounters extractCounters( Map<String,Value> metadata )
    {
        Value countersValue = metadata.get( "stats" );
        if ( countersValue != null )
        {
            return new InternalSummaryCounters(
                    counterValue( countersValue, "nodes-created" ),
                    counterValue( countersValue, "nodes-deleted" ),
                    counterValue( countersValue, "relationships-created" ),
                    counterValue( countersValue, "relationships-deleted" ),
                    counterValue( countersValue, "properties-set" ),
                    counterValue( countersValue, "labels-added" ),
                    counterValue( countersValue, "labels-removed" ),
                    counterValue( countersValue, "indexes-added" ),
                    counterValue( countersValue, "indexes-removed" ),
                    counterValue( countersValue, "constraints-added" ),
                    counterValue( countersValue, "constraints-removed" )
            );
        }
        return null;
    }

    private static int counterValue( Value countersValue, String name )
    {
        Value value = countersValue.get( name );
        return value.isNull() ? 0 : value.asInt();
    }

    private static Plan extractPlan( Map<String,Value> metadata )
    {
        Value planValue = metadata.get( "plan" );
        if ( planValue != null )
        {
            return InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( planValue );
        }
        return null;
    }

    private static ProfiledPlan extractProfiledPlan( Map<String,Value> metadata )
    {
        Value profiledPlanValue = metadata.get( "profile" );
        if ( profiledPlanValue != null )
        {
            return InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( profiledPlanValue );
        }
        return null;
    }

    private static List<Notification> extractNotifications( Map<String,Value> metadata )
    {
        Value notificationsValue = metadata.get( "notifications" );
        if ( notificationsValue != null )
        {
            return notificationsValue.asList( InternalNotification.VALUE_TO_NOTIFICATION );
        }
        return Collections.emptyList();
    }

    private static long extractResultConsumedAfter( Map<String,Value> metadata )
    {
        Value resultConsumedAfterValue = metadata.get( "result_consumed_after" );
        if ( resultConsumedAfterValue != null )
        {
            return resultConsumedAfterValue.asLong();
        }
        return -1;
    }
}
