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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.lang.String.format;

public class MicrometerConnectionPoolMetrics implements ConnectionPoolMetricsListener, ConnectionPoolMetrics
{
    public static final String PREFIX = "neo4j.driver.connections";
    public static final String IN_USE = PREFIX + ".in.use";
    public static final String IDLE = PREFIX + ".idle";
    public static final String CREATING = PREFIX + ".creating";
    public static final String CREATED = PREFIX + ".created";
    public static final String FAILED = PREFIX + ".failed";
    public static final String CLOSED = PREFIX + ".closed";
    public static final String ACQUIRING = PREFIX + ".acquiring";
    public static final String ACQUIRED = PREFIX + ".acquired";
    public static final String ACQUISITION_TIMEOUT = PREFIX + ".acquisition.timeout";
    public static final String ACQUISITION = PREFIX + ".acquisition";
    public static final String CREATION = PREFIX + ".creation";
    public static final String USAGE = PREFIX + ".usage";
    public static final String RELEASED = PREFIX + ".released";

    private final BoltServerAddress address;
    private final ConnectionPool pool;

    private final String id;

    private final AtomicInteger creating = new AtomicInteger();
    private final Counter created;
    private final Counter failedToCreate;
    private final Counter closed;
    private final AtomicInteger acquiring = new AtomicInteger();
    private final Counter acquired;
    private final Counter timedOutToAcquire;
    private final Timer totalAcquisitionTimer;
    private final Timer totalConnectionTimer;
    private final Timer totalInUseTimer;
    private final Counter released;

    MicrometerConnectionPoolMetrics( String poolId, BoltServerAddress address, ConnectionPool pool, MeterRegistry registry )
    {
        this( poolId, address, pool, registry, Tags.empty() );
    }

    MicrometerConnectionPoolMetrics( String poolId, BoltServerAddress address, ConnectionPool pool, MeterRegistry registry, Iterable<Tag> initialTags )
    {
        Objects.requireNonNull( poolId );
        Objects.requireNonNull( address );
        Objects.requireNonNull( pool );
        Objects.requireNonNull( registry );

        this.id = poolId;
        this.address = address;
        this.pool = pool;
        Iterable<Tag> tags = Tags.concat( initialTags,
                                          "address", String.format( "%s:%d", address.connectionHost(), address.port() ) );

        Gauge.builder( IN_USE, this::inUse ).tags( tags ).register( registry );
        Gauge.builder( IDLE, this::idle ).tags( tags ).register( registry );
        Gauge.builder( CREATING, creating, AtomicInteger::get ).tags( tags ).register( registry );
        created = Counter.builder( CREATED ).tags( tags ).register( registry );
        failedToCreate = Counter.builder( FAILED ).tags( tags ).register( registry );
        closed = Counter.builder( CLOSED ).tags( tags ).register( registry );
        Gauge.builder( ACQUIRING, acquiring, AtomicInteger::get ).tags( tags ).register( registry );
        acquired = Counter.builder( ACQUIRED ).tags( tags ).register( registry );
        timedOutToAcquire = Counter.builder( ACQUISITION_TIMEOUT ).tags( tags ).register( registry );
        totalAcquisitionTimer = Timer.builder( ACQUISITION ).tags( tags ).register( registry );
        totalConnectionTimer = Timer.builder( CREATION ).tags( tags ).register( registry );
        totalInUseTimer = Timer.builder( USAGE ).tags( tags ).register( registry );
        released = Counter.builder( RELEASED ).tags( tags ).register( registry );
    }

    @Override
    public void beforeCreating( ListenerEvent<?> connEvent )
    {
        creating.incrementAndGet();
        connEvent.start();
    }

    @Override
    public void afterFailedToCreate()
    {
        failedToCreate.increment();
        creating.decrementAndGet();
    }

    @Override
    public void afterCreated( ListenerEvent<?> connEvent )
    {
        creating.decrementAndGet();
        created.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) connEvent).getSample();
        sample.stop( totalConnectionTimer );
    }

    @Override
    public void afterClosed()
    {
        closed.increment();
    }

    @Override
    public void beforeAcquiringOrCreating( ListenerEvent<?> acquireEvent )
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
    public void afterAcquiredOrCreated( ListenerEvent<?> acquireEvent )
    {
        acquired.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) acquireEvent).getSample();
        sample.stop( totalAcquisitionTimer );
    }

    @Override
    public void afterTimedOutToAcquireOrCreate()
    {
        timedOutToAcquire.increment();
    }

    @Override
    public void acquired( ListenerEvent<?> inUseEvent )
    {
        inUseEvent.start();
    }

    @Override
    public void released( ListenerEvent<?> inUseEvent )
    {
        released.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) inUseEvent).getSample();
        sample.stop( totalInUseTimer );
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
        return toLong( created );
    }

    @Override
    public long failedToCreate()
    {
        return toLong( failedToCreate );
    }

    @Override
    public long closed()
    {
        return toLong( closed );
    }

    @Override
    public int acquiring()
    {
        return acquiring.get();
    }

    @Override
    public long acquired()
    {
        return toLong( acquired );
    }

    @Override
    public long timedOutToAcquire()
    {
        return toLong( timedOutToAcquire );
    }

    @Override
    public long totalAcquisitionTime()
    {
        return (long) totalAcquisitionTimer.totalTime( TimeUnit.MILLISECONDS );
    }

    @Override
    public long totalConnectionTime()
    {
        return (long) totalConnectionTimer.totalTime( TimeUnit.MILLISECONDS );
    }

    @Override
    public long totalInUseTime()
    {
        return (long) totalInUseTimer.totalTime( TimeUnit.MILLISECONDS );
    }

    @Override
    public long totalInUseCount()
    {
        return toLong( released );
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

    private long toLong( Counter counter )
    {
        return (long) counter.count();
    }
}
