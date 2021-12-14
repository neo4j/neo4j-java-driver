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

    // creating = created + failedToCreate
    private final AtomicInteger creating = new AtomicInteger();
    private final Counter failedToCreate;

    // acquiring = acquired + timedOutToAcquire + failedToAcquireDueToOtherFailures (which we do not keep track)
    private final AtomicInteger acquiring = new AtomicInteger();
    private final Counter timedOutToAcquire;

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
        failedToCreate = Counter.builder(PREFIX + ".failed").tags(this.tags).register(this.registry);
        timedOutToAcquire = Counter.builder(PREFIX + ".acquisition.timeout").tags(this.tags).register(this.registry);
        closed = Counter.builder(PREFIX + ".closed").tags(this.tags).register(this.registry);
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
        Timer.Sample sample = ((MicrometerTimerListenerEvent) connEvent).getSample();
        sample.stop(Timer.builder(PREFIX + ".creation").tags(this.tags).register(this.registry));
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
        Timer.Sample sample = ((MicrometerTimerListenerEvent) acquireEvent).getSample();
        // We can't time failed acquisitions currently
        // Same for creation Timer and in-use Timer
        sample.stop(Timer.builder(PREFIX + ".acquisition").tags(this.tags).register(this.registry));
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
        Timer.Sample sample = ((MicrometerTimerListenerEvent) inUseEvent).getSample();
        sample.stop(Timer.builder(PREFIX + ".usage").tags(this.tags).register(registry));
    }

    @Override
    public String id() {
        return this.id;
    }

    // no-ops below here
    @Override
    public int inUse() {
        return 0;
    }

    @Override
    public int idle() {
        return 0;
    }

    @Override
    public int creating() {
        return 0;
    }

    @Override
    public long created() {
        return 0;
    }

    @Override
    public long failedToCreate() {
        return 0;
    }

    @Override
    public long closed() {
        return 0;
    }

    @Override
    public int acquiring() {
        return 0;
    }

    @Override
    public long acquired() {
        return 0;
    }

    @Override
    public long timedOutToAcquire() {
        return 0;
    }

    @Override
    public long totalAcquisitionTime() {
        return 0;
    }

    @Override
    public long totalConnectionTime() {
        return 0;
    }

    @Override
    public long totalInUseTime() {
        return 0;
    }

    @Override
    public long totalInUseCount() {
        return 0;
    }
}
