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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetrics;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetricsTracker;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetricsTrackerProvider;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;

import static java.lang.String.format;
import static org.neo4j.driver.internal.metrics.InternalMetrics.serverAddressToUniqueName;

public class InternalConnectionPoolMetrics implements ConnectionPoolMetrics, ConnectionPoolMetricsListener
{
    private final Clock clock;
    private final BoltServerAddress address;
    private final ConnectionPool pool;

    private final AtomicLong closed = new AtomicLong();


    // creating = created + failedToCreate
    private final AtomicInteger creating = new AtomicInteger();
    private final AtomicLong created = new AtomicLong();
    private final AtomicLong failedToCreate = new AtomicLong();


    // acquiring = acquired + timedOutToAcquire + failedToAcquireDueToOtherFailures
    private final AtomicInteger acquiring = new AtomicInteger();
    private final AtomicLong acquired = new AtomicLong();
    private final AtomicLong timedOutToAcquire = new AtomicLong();


    private final ConnectionPoolMetricsTrackerProvider trackerProvider;

    public InternalConnectionPoolMetrics( BoltServerAddress address, ConnectionPool pool, Clock clock, ConnectionPoolMetricsTrackerProvider trackerProvider )
    {
        Objects.requireNonNull( address );
        Objects.requireNonNull( pool );

        this.address = address;
        this.pool = pool;
        this.clock = clock;
        this.trackerProvider = trackerProvider;
    }

    @Override
    public void beforeCreating( ListenerEvent connEvent )
    {
        creating.incrementAndGet();
        connEvent.start( clock.millis() );
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
        long elapsed = connEvent.elapsed( clock.millis() );

        metricsTracker().recordConnectionTime( elapsed );
    }

    @Override
    public void afterClosed()
    {
        closed.incrementAndGet();
    }

    @Override
    public void beforeAcquiringOrCreating( ListenerEvent acquireEvent )
    {
        acquireEvent.start( clock.millis() );
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
        long elapsed = acquireEvent.elapsed( clock.millis() );

        metricsTracker().recordAcquisitionTime( elapsed );

        acquired.incrementAndGet();
    }

    @Override
    public void afterTimedOutToAcquireOrCreate()
    {
        timedOutToAcquire.incrementAndGet();
    }

    @Override
    public void acquired( ListenerEvent inUseEvent )
    {
        inUseEvent.start( clock.millis() );
    }

    @Override
    public void released( ListenerEvent inUseEvent )
    {
        long elapsed = inUseEvent.elapsed( clock.millis() );

        metricsTracker().recordInUseTime( elapsed );
    }

    private ConnectionPoolMetricsTracker metricsTracker()
    {
        return trackerProvider.metricsTrackerForPool( this.id() );
    }

    @Override
    public String id()
    {
        return serverAddressToUniqueName( address );
    }

    @Override
    public PoolStatus poolStatus()
    {
        if ( pool.isOpen( address ) )
        {
            return PoolStatus.OPEN;
        }
        else
        {
            return PoolStatus.CLOSED;
        }
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
        return format( "[created=%s, closed=%s, creating=%s, failedToCreate=%s, acquiring=%s, acquired=%s, " +
                        "timedOutToAcquire=%s, inUse=%s, idle=%s, poolStatus=%s]",
                created(), closed(), creating(), failedToCreate(), acquiring(), acquired(),
                timedOutToAcquire(), inUse(), idle(), poolStatus());
    }
}
