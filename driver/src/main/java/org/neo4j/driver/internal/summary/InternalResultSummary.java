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
package org.neo4j.driver.internal.summary;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Query;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.SummaryCounters;

public class InternalResultSummary implements ResultSummary
{
    private final Query query;
    private final ServerInfo serverInfo;
    private final QueryType queryType;
    private final SummaryCounters counters;
    private final Plan plan;
    private final ProfiledPlan profile;
    private final List<Notification> notifications;
    private final long resultAvailableAfter;
    private final long resultConsumedAfter;
    private final DatabaseInfo databaseInfo;

    public InternalResultSummary(Query query, ServerInfo serverInfo, DatabaseInfo databaseInfo, QueryType queryType,
                                 SummaryCounters counters, Plan plan, ProfiledPlan profile, List<Notification> notifications, long resultAvailableAfter, long resultConsumedAfter )
    {
        this.query = query;
        this.serverInfo = serverInfo;
        this.databaseInfo = databaseInfo;
        this.queryType = queryType;
        this.counters = counters;
        this.plan = resolvePlan( plan, profile );
        this.profile = profile;
        this.notifications = notifications;
        this.resultAvailableAfter = resultAvailableAfter;
        this.resultConsumedAfter = resultConsumedAfter;
    }

    @Override
    public Query query()
    {
        return query;
    }

    @Override
    public SummaryCounters counters()
    {
        return counters == null ? InternalSummaryCounters.EMPTY_STATS : counters;
    }

    @Override
    public QueryType queryType()
    {
        return queryType;
    }

    @Override
    public boolean hasPlan()
    {
        return plan != null;
    }

    @Override
    public boolean hasProfile()
    {
        return profile != null;
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
        return notifications == null ? Collections.<Notification>emptyList() : notifications;
    }

    @Override
    public long resultAvailableAfter( TimeUnit unit )
    {
        return resultAvailableAfter == -1 ? resultAvailableAfter
                                          : unit.convert( resultAvailableAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public long resultConsumedAfter( TimeUnit unit )
    {
        return resultConsumedAfter == -1 ? resultConsumedAfter
                                         : unit.convert( resultConsumedAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public ServerInfo server()
    {
        return serverInfo;
    }

    @Override
    public DatabaseInfo database()
    {
        return databaseInfo;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InternalResultSummary that = (InternalResultSummary) o;
        return resultAvailableAfter == that.resultAvailableAfter &&
               resultConsumedAfter == that.resultConsumedAfter &&
               Objects.equals(query, that.query) &&
               Objects.equals( serverInfo, that.serverInfo ) &&
               queryType == that.queryType &&
               Objects.equals( counters, that.counters ) &&
               Objects.equals( plan, that.plan ) &&
               Objects.equals( profile, that.profile ) &&
               Objects.equals( notifications, that.notifications );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, serverInfo, queryType, counters, plan, profile, notifications,
                resultAvailableAfter, resultConsumedAfter );
    }

    @Override
    public String toString()
    {
        return "InternalResultSummary{" +
               "query=" + query +
               ", serverInfo=" + serverInfo +
               ", databaseInfo=" + databaseInfo +
               ", queryType=" + queryType +
               ", counters=" + counters +
               ", plan=" + plan +
               ", profile=" + profile +
               ", notifications=" + notifications +
               ", resultAvailableAfter=" + resultAvailableAfter +
               ", resultConsumedAfter=" + resultConsumedAfter +
               '}';
    }

    /**
     * Profiled plan is a superset of plan. This method returns profiled plan if plan is {@code null}.
     *
     * @param plan the given plan, possibly {@code null}.
     * @param profiledPlan the given profiled plan, possibly {@code null}.
     * @return available plan.
     */
    private static Plan resolvePlan( Plan plan, ProfiledPlan profiledPlan )
    {
        return plan == null ? profiledPlan : plan;
    }
}
