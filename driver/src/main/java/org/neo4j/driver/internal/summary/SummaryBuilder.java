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
package org.neo4j.driver.internal.summary;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;

public class SummaryBuilder implements Collector
{
    private final Statement statement;

    private StatementType type = null;
    private SummaryCounters statistics = null;
    private Plan plan = null;
    private ProfiledPlan profile;
    private List<Notification> notifications = null;
    private long resultAvailableAfter = -1L;
    private long resultConsumedAfter = -1L;

    public SummaryBuilder( Statement statement )
    {
        this.statement = statement;
    }

    @Override
    public void keys( String[] names )
    {
        // intentionally empty
    }

    @Override
    public void record( Value[] fields )
    {
        // intentionally empty
    }

    public void statementType( StatementType type )
    {
        if ( this.type == null )
        {
            this.type = type;
        }
        else
        {
            throw new ClientException( "Received statement type twice" );
        }
    }

    public void statementStatistics( SummaryCounters statistics )
    {
        if ( this.statistics == null )
        {
            this.statistics = statistics;
        }
        else
        {
            throw new ClientException( "Received statement statistics twice" );
        }
    }

    @Override
    public void plan( Plan plan )
    {
        if ( this.plan == null )
        {
            this.plan = plan;
        }
        else
        {
            throw new ClientException( "Received plan twice" );
        }
    }

    @Override
    public void profile( ProfiledPlan plan )
    {
        if ( this.plan == null && this.profile == null )
        {
            this.profile = plan;
            this.plan = plan;
        }
        else
        {
            throw new ClientException( "Received plan twice" );
        }
    }

    @Override
    public void notifications( List<Notification> notifications )
    {
        if( this.notifications == null )
        {
            this.notifications = notifications;
        }
        else
        {
            throw new ClientException( "Received notifications twice" );
        }
    }

    @Override
    public void bookmark( String bookmark )
    {
        // intentionally empty
    }

    @Override
    public void done()
    {
        // intentionally empty
    }

    @Override
    public void doneSuccess()
    {
        // intentionally empty
    }

    @Override
    public void doneFailure( Neo4jException erro )
    {
        // intentionally empty
    }

    @Override
    public void doneIgnored()
    {
        // intentionally empty
    }

    @Override
    public void resultAvailableAfter( long l )
    {
      this.resultAvailableAfter = l;
    }

    @Override
    public void resultConsumedAfter( long l )
    {
        this.resultConsumedAfter = l;
    }

    @Override
    public void server( String server )
    {
        // intentionally empty
    }

    public ResultSummary build()
    {
        return new ResultSummary()
        {
            @Override
            public Statement statement()
            {
                return statement;
            }

            @Override
            public SummaryCounters counters()
            {
                return statistics == null ? InternalSummaryCounters.EMPTY_STATS : statistics;
            }

            @Override
            public StatementType statementType()
            {
                return type;
            }

            @Override
            public boolean hasProfile()
            {
                return profile != null;
            }

            @Override
            public boolean hasPlan()
            {
                return plan != null;
            }

            @Override
            public Plan plan()
            {
                return plan;
            }

            @Override
            public ProfiledPlan profile()
            {
                return profile;
            }

            @Override
            public List<Notification> notifications()
            {
                return notifications == null ? new ArrayList<Notification>() : notifications;
            }

            @Override
            public long resultAvailableAfter( TimeUnit timeUnit )
            {
                return timeUnit.convert( resultAvailableAfter, TimeUnit.MILLISECONDS );
            }

            @Override
            public long resultConsumedAfter( TimeUnit timeUnit )
            {
                return timeUnit.convert( resultConsumedAfter, TimeUnit.MILLISECONDS );
            }
        };
    }
}
