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
package org.neo4j.driver.internal.metrics;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.lang.String.format;

public class InternalConnectionPoolMetrics implements ConnectionPoolMetrics, ConnectionPoolMetricsListener
{
    private final BoltServerAddress address;
    private final ConnectionPool pool;

    private final AtomicLong closed = new AtomicLong();

    // creating = created + failedToCreate
    private final AtomicInteger creating = new AtomicInteger();
    private final AtomicLong created = new AtomicLong();
    private final AtomicLong failedToCreate = new AtomicLong();

    // acquiring = acquired + timedOutToAcquire + failedToAcquireDueToOtherFailures (which we do not keep track)
    private final AtomicInteger acquiring = new AtomicInteger();
    private final AtomicLong acquired = new AtomicLong();
    private final AtomicLong timedOutToAcquire = new AtomicLong();

    private final AtomicLong totalAcquisitionTime = new AtomicLong();
    private final AtomicLong totalConnectionTime = new AtomicLong();
    private final AtomicLong totalInUseTime = new AtomicLong();

    private final AtomicLong totalInUseCount = new AtomicLong();
    private final String id;

    InternalConnectionPoolMetrics( String poolId, BoltServerAddress address, ConnectionPool pool )
    {
        Objects.requireNonNull( address );
        Objects.requireNonNull( pool );

        this.id = poolId;
        this.address = address;
        this.pool = pool;
    }

    @Override
    public void beforeCreating( ListenerEvent connEvent )
    {
        creating.incrementAndGet();
        connEvent.start();
    }

    @Override
    public void afterFailedToCreate()
    {
        failedToCreate.incrementAndGet();
        creating.decrementAndGet();
    }

    @Override
    public void afterCreated( ListenerEvent connEvent )
    {
        created.incrementAndGet();
        creating.decrementAndGet();
        long elapsed = connEvent.elapsed();

        totalConnectionTime.addAndGet( elapsed );
    }

    @Override
    public void afterClosed()
    {
        closed.incrementAndGet();
    }

    @Override
    public void beforeAcquiringOrCreating( ListenerEvent acquireEvent )
    {
        acquireEvent.start();
        acquiring.incrementAndGet();
    }

    @Override
    public void afterAcquiringOrCreating()
    {
        acquiring.decrementAndGet();
    }

    @Override
    public void afterAcquiredOrCreated( ListenerEvent acquireEvent )
    {
        acquired.incrementAndGet();
        long elapsed = acquireEvent.elapsed();

        totalAcquisitionTime.addAndGet( elapsed );
    }

    @Override
    public void afterTimedOutToAcquireOrCreate()
    {
        timedOutToAcquire.incrementAndGet();
    }

    @Override
    public void acquired( ListenerEvent inUseEvent )
    {
        inUseEvent.start();
    }

    @Override
    public void released( ListenerEvent inUseEvent )
    {
        totalInUseCount.incrementAndGet();
        long elapsed = inUseEvent.elapsed();

        totalInUseTime.addAndGet( elapsed );
    }

    @Override
    public String id()
    {
        return this.id;
    }

    @Override
    public int inUse()
    {
        return pool.inUseConnections( address );
    }

    @Override
    public int idle()
    {
        return pool.idleConnections( address );
    }

    @Override
    public int creating()
    {
        return creating.get();
    }

    @Override
    public long created()
    {
        return created.get();
    }

    @Override
    public long failedToCreate()
    {
        return failedToCreate.get();
    }

    @Override
    public long timedOutToAcquire()
    {
        return timedOutToAcquire.get();
    }

    @Override
    public long totalAcquisitionTime()
    {
        return totalAcquisitionTime.get();
    }

    @Override
    public long totalConnectionTime()
    {
        return totalConnectionTime.get();
    }

    @Override
    public long totalInUseTime()
    {
        return totalInUseTime.get();
    }

    @Override
    public long totalInUseCount()
    {
        return totalInUseCount.get();
    }

    @Override
    public long closed()
    {
        return closed.get();
    }

    @Override
    public int acquiring()
    {
        return acquiring.get();
    }

    @Override
    public long acquired()
    {
        return this.acquired.get();
    }


    @Override
    public String toString()
    {
        return format( "%s=[created=%s, closed=%s, creating=%s, failedToCreate=%s, acquiring=%s, acquired=%s, " +
                        "timedOutToAcquire=%s, inUse=%s, idle=%s, " +
                        "totalAcquisitionTime=%s, totalConnectionTime=%s, totalInUseTime=%s, totalInUseCount=%s]",
                id(), created(), closed(), creating(), failedToCreate(), acquiring(), acquired(),
                timedOutToAcquire(), inUse(), idle(),
                totalAcquisitionTime(), totalConnectionTime(), totalInUseTime(), totalInUseCount() );
    }
}
