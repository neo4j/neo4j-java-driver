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

import io.micrometer.core.instrument.*;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MicrometerConnectionPoolMetrics implements ConnectionPoolMetricsListener, ConnectionPoolMetrics
{
    private static final String PREFIX = "neo4j.driver.connections";

    // address and pool are not used.
    private final BoltServerAddress address;
    private final ConnectionPool pool;

    private final Counter closed;

    private final AtomicInteger creating = new AtomicInteger();
    private final Counter failedToCreate;
    private final Counter created;

    // acquiring = acquired + timedOutToAcquire + failedToAcquireDueToOtherFailures (which we do not keep track)
    private final AtomicInteger acquiring = new AtomicInteger();
    private final Counter timedOutToAcquire;
    private final AtomicLong totalAcquisitionTime = new AtomicLong();
    private final AtomicLong totalConnectionTime = new AtomicLong();
    private final AtomicLong totalInUseTime = new AtomicLong();
    private final Counter acquired;
    private final Counter totalInUse;

    private final String id;
    private final MeterRegistry registry;
    private final Iterable<Tag> tags;

    MicrometerConnectionPoolMetrics(String poolId, BoltServerAddress address, ConnectionPool pool, MeterRegistry registry )
    {
        this(poolId, address, pool, registry, Tags.empty());
    }

    MicrometerConnectionPoolMetrics(String poolId, BoltServerAddress address, ConnectionPool pool, MeterRegistry registry, Iterable<Tag> tags ) {
        Objects.requireNonNull( address );
        Objects.requireNonNull( pool );
        Objects.requireNonNull( registry );

        this.id = poolId;
        this.address = address;
        this.pool = pool;
        this.registry = registry;
        this.tags = Tags.concat(tags, "poolId", poolId);

        Gauge.builder(PREFIX + ".creating", creating, AtomicInteger::get).tags(this.tags).register(this.registry);
        Gauge.builder(PREFIX + ".acquiring", acquiring, AtomicInteger::get).tags(this.tags).register(this.registry);
        Gauge.builder(PREFIX + ".inUse", this::inUse).tags(this.tags).register(this.registry);
        Gauge.builder(PREFIX + ".idle", this::idle).tags(this.tags).register(this.registry);

        failedToCreate = Counter.builder(PREFIX + ".failed").tags(this.tags).register(this.registry);
        timedOutToAcquire = Counter.builder(PREFIX + ".acquisition.timeout").tags(this.tags).register(this.registry);
        closed = Counter.builder(PREFIX + ".closed").tags(this.tags).register(this.registry);
        created = Counter.builder(PREFIX + ".created").tags(this.tags).register(this.registry);
        acquired = Counter.builder(PREFIX + ".acquired").tags(this.tags).register(this.registry);
        totalInUse = Counter.builder(PREFIX + ".inUse").tags(this.tags).register(this.registry);
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
        failedToCreate.increment();
        creating.decrementAndGet();
    }

    @Override
    public void afterCreated( ListenerEvent connEvent )
    {
        creating.decrementAndGet();
        created.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) connEvent).getSample();
        long elapsed = sample.stop(Timer.builder(PREFIX + ".creation").tags(this.tags).register(this.registry));
        totalConnectionTime.addAndGet( elapsed );
    }

    @Override
    public void afterClosed()
    {
        closed.increment();
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
        acquired.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) acquireEvent).getSample();
        // We can't time failed acquisitions currently
        // Same for creation Timer and in-use Timer
        long elapsed = sample.stop(Timer.builder(PREFIX + ".acquisition").tags(this.tags).register(this.registry));
        totalAcquisitionTime.addAndGet(elapsed);
    }

    @Override
    public void afterTimedOutToAcquireOrCreate()
    {
        // if we could access the ListenerEvent here, we could use the Timer with acquisition timeouts
        timedOutToAcquire.increment();
    }

    @Override
    public void acquired( ListenerEvent inUseEvent )
    {
        inUseEvent.start();
    }

    @Override
    public void released( ListenerEvent inUseEvent )
    {
        totalInUse.increment();
        Timer.Sample sample = ((MicrometerTimerListenerEvent) inUseEvent).getSample();
        long elapsed = sample.stop(Timer.builder(PREFIX + ".usage").tags(this.tags).register(registry));
        totalInUseTime.addAndGet( elapsed );
    }

    @Override
    public String id() {
        return this.id;
    }

    // no-ops below here
    @Override
    public int inUse() {
        return pool.inUseConnections(address);
    }

    @Override
    public int idle() {
        return pool.idleConnections(address);
    }

    @Override
    public int creating() {
        return creating.get();
    }

    @Override
    public long created() {
        return ((Double) created.count()).longValue();
    }

    @Override
    public long failedToCreate() {
        return ((Double) failedToCreate.count()).longValue();
    }

    @Override
    public long closed() {
        return ((Double) closed.count()).longValue();
    }

    @Override
    public int acquiring() {
        return acquiring.get();
    }

    @Override
    public long acquired() {
        return ((Double) acquired.count()).longValue();
    }

    @Override
    public long timedOutToAcquire() {
        return ((Double) timedOutToAcquire.count()).longValue();
    }

    @Override
    public long totalAcquisitionTime() {
        return totalAcquisitionTime.get();
    }

    @Override
    public long totalConnectionTime() {
        return totalConnectionTime.get();
    }

    @Override
    public long totalInUseTime() {
        return totalInUseTime.get();
    }

    @Override
    public long totalInUseCount() {
        return ((Double) totalInUse.count()).longValue();
    }
}
