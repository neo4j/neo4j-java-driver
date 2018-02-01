/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

package org.neo4j.driver.internal.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetrics;
import org.neo4j.driver.internal.metrics.spi.Histogram;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.lang.String.format;
import static org.neo4j.driver.internal.metrics.InternalDriverMetrics.serverAddressToUniqueName;

public class InternalConnectionPoolMetrics implements ConnectionPoolMetrics, ConnectionPoolListener
{
    private final BoltServerAddress address;
    private final ConnectionPool pool;

    private AtomicLong created = new AtomicLong();
    private AtomicLong closed = new AtomicLong();
    private AtomicInteger toCreate = new AtomicInteger();
    private AtomicLong failedToCreate = new AtomicLong();

    // TODO
    private Histogram histogram;

    public InternalConnectionPoolMetrics(BoltServerAddress address, ConnectionPool pool, long connAcquisitionTimeoutMs)
    {
        this.address = address;
        this.pool = pool;
        this.histogram = null;
    }

    @Override
    public void beforeCreating()
    {
        toCreate.incrementAndGet();
    }

    @Override
    public void afterCreatedSuccessfully()
    {
        created.incrementAndGet();
        toCreate.decrementAndGet();
    }

    @Override
    public void afterFailedToCreate()
    {
        failedToCreate.incrementAndGet();
        toCreate.decrementAndGet();
    }

    @Override
    public void afterClosed()
    {
        closed.incrementAndGet();
    }

    @Override
    public void beforeAcquire( ListenerEvent listenerEvent )
    {
        listenerEvent.start();
    }

    @Override
    public void afterAcquire( ListenerEvent listenerEvent )
    {
        long elapsed = listenerEvent.elapsed();
        // TODO register in histogram
    }

    @Override
    public String uniqueName()
    {
        return serverAddressToUniqueName( address );
    }

    @Override
    public String poolStatus()
    {
        if ( pool.isOpen() )
        {
            return "\"Open\"";
        }
        else
        {
            return "\"Closed\"";
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
    public int toCreate()
    {
        return toCreate.get();
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
    public long closed()
    {
        return closed.get();
    }

    @Override
    public Histogram acquisitionTimeHistogram()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return format( "[created=%s, closed=%s, toCreate=%s, failedToCreate=%s inUse=%s, idle=%s, poolStatus=%s]",
                created(), closed(), toCreate(), failedToCreate(), inUse(), idle(), poolStatus() );
    }
}
