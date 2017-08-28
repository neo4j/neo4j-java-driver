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
package org.neo4j.driver.internal.summary;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.ServerInfo;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;

public class InternalResultSummary implements ResultSummary
{
    private final Statement statement;
    private final ServerInfo serverInfo;
    private final StatementType statementType;
    private final SummaryCounters counters;
    private final Plan plan;
    private final ProfiledPlan profile;
    private final List<Notification> notifications;
    private final long resultAvailableAfter;
    private final long resultConsumedAfter;

    public InternalResultSummary( Statement statement, ServerInfo serverInfo, StatementType statementType,
            SummaryCounters counters, Plan plan, ProfiledPlan profile, List<Notification> notifications,
            long resultAvailableAfter, long resultConsumedAfter )
    {
        this.statement = statement;
        this.serverInfo = serverInfo;
        this.statementType = statementType;
        this.counters = counters;
        this.plan = resolvePlan( plan, profile );
        this.profile = profile;
        this.notifications = notifications;
        this.resultAvailableAfter = resultAvailableAfter;
        this.resultConsumedAfter = resultConsumedAfter;
    }

    @Override
    public Statement statement()
    {
        return statement;
    }

    @Override
    public SummaryCounters counters()
    {
        return counters == null ? InternalSummaryCounters.EMPTY_STATS : counters;
    }

    @Override
    public StatementType statementType()
    {
        return statementType;
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
        return unit.convert( resultAvailableAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public long resultConsumedAfter( TimeUnit unit )
    {
        return unit.convert( resultConsumedAfter, TimeUnit.MILLISECONDS );
    }

    @Override
    public ServerInfo server()
    {
        return serverInfo;
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
               Objects.equals( statement, that.statement ) &&
               Objects.equals( serverInfo, that.serverInfo ) &&
               statementType == that.statementType &&
               Objects.equals( counters, that.counters ) &&
               Objects.equals( plan, that.plan ) &&
               Objects.equals( profile, that.profile ) &&
               Objects.equals( notifications, that.notifications );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statement, serverInfo, statementType, counters, plan, profile, notifications,
                resultAvailableAfter, resultConsumedAfter );
    }

    @Override
    public String toString()
    {
        return "InternalResultSummary{" +
               "statement=" + statement +
               ", serverInfo=" + serverInfo +
               ", statementType=" + statementType +
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
