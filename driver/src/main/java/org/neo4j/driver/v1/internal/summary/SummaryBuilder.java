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
package org.neo4j.driver.v1.internal.summary;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.UpdateStatistics;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.spi.StreamCollector;

public class SummaryBuilder implements StreamCollector
{
    private final Statement statement;

    private StatementType type = null;
    private UpdateStatistics statistics = null;
    private Plan plan = null;
    private ProfiledPlan profile;
    private List<Notification> notifications = null;

    public SummaryBuilder( Statement statement )
    {
        this.statement = statement;
    }

    @Override
    public void fieldNames( String[] names )
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

    public void statementStatistics( UpdateStatistics statistics )
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
            public UpdateStatistics updateStatistics()
            {
                return statistics == null ? SimpleUpdateStatistics.EMPTY_STATS : statistics;
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
        };
    }
}
