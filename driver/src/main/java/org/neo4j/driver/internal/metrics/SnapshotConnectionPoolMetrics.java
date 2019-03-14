/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.metrics;

import org.neo4j.driver.ConnectionPoolMetrics;

import static java.lang.String.format;

public class SnapshotConnectionPoolMetrics implements ConnectionPoolMetrics
{
    private final long acquired;
    private final String id;
    private final int acquiring;
    private final PoolStatus poolStatus;
    private final int idle;
    private final int inUse;
    private final int creating;
    private final long created;
    private final long failedToCreate;
    private final long closed;
    private final long timedOutToAcquire;
    private final long totalAcquisitionTime;
    private final long totalConnectionTime;
    private final long totalInUseTime;
    private final long totalInUseCount;

    public SnapshotConnectionPoolMetrics( ConnectionPoolMetrics other )
    {
        id = other.id();
        acquired = other.acquired();
        acquiring = other.acquiring();
        poolStatus = other.poolStatus();
        idle = other.idle();
        inUse = other.inUse();
        creating = other.creating();
        created = other.created();
        failedToCreate = other.failedToCreate();
        closed = other.closed();
        timedOutToAcquire = other.timedOutToAcquire();
        totalAcquisitionTime = other.totalAcquisitionTime();
        totalConnectionTime = other.totalConnectionTime();
        totalInUseTime = other.totalInUseTime();
        totalInUseCount = other.totalInUseCount();
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public PoolStatus poolStatus()
    {
        return poolStatus;
    }

    @Override
    public int inUse()
    {
        return inUse;
    }

    @Override
    public int idle()
    {
        return idle;
    }

    @Override
    public int creating()
    {
        return creating;
    }

    @Override
    public long created()
    {
        return created;
    }

    @Override
    public long failedToCreate()
    {
        return failedToCreate;
    }

    @Override
    public long closed()
    {
        return closed;
    }

    @Override
    public int acquiring()
    {
        return acquiring;
    }

    @Override
    public long acquired()
    {
        return acquired;
    }

    @Override
    public long timedOutToAcquire()
    {
        return timedOutToAcquire;
    }

    @Override
    public long totalAcquisitionTime()
    {
        return totalAcquisitionTime;
    }

    @Override
    public long totalConnectionTime()
    {
        return totalConnectionTime;
    }

    @Override
    public long totalInUseTime()
    {
        return totalInUseTime;
    }

    @Override
    public long totalInUseCount()
    {
        return totalInUseCount;
    }

    @Override
    public ConnectionPoolMetrics snapshot()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return format( "[created=%s, closed=%s, creating=%s, failedToCreate=%s, acquiring=%s, acquired=%s, " +
                        "timedOutToAcquire=%s, inUse=%s, idle=%s, poolStatus=%s, " +
                        "totalAcquisitionTime=%s, totalConnectionTime=%s, totalInUseTime=%s, totalInUseCount=%s]",
                created(), closed(), creating(), failedToCreate(), acquiring(), acquired(),
                timedOutToAcquire(), inUse(), idle(), poolStatus(),
                totalAcquisitionTime(), totalConnectionTime(), totalInUseTime(), totalInUseCount() );
    }
}
