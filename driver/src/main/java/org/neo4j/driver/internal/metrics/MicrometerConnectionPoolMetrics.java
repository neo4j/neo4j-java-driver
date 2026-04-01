/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.lang.String.format;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.driver.ConnectionPoolMetrics;

final class MicrometerConnectionPoolMetrics implements ConnectionPoolMetrics {
    public static final String PREFIX = "neo4j.driver.connections";
    public static final String IN_USE = PREFIX + ".in.use";
    public static final String IDLE = PREFIX + ".idle";
    public static final String CREATING = PREFIX + ".creating";
    public static final String FAILED = PREFIX + ".failed";
    public static final String CLOSED = PREFIX + ".closed";
    public static final String ACQUIRING = PREFIX + ".acquiring";
    public static final String ACQUISITION_TIMEOUT = PREFIX + ".acquisition.timeout";
    public static final String ACQUISITION = PREFIX + ".acquisition";
    public static final String CREATION = PREFIX + ".creation";
    public static final String USAGE = PREFIX + ".usage";

    private final AtomicInteger inUse = new AtomicInteger();
    private final AtomicInteger idle = new AtomicInteger();

    private final String id;

    private final AtomicInteger creating = new AtomicInteger();
    private final Counter failedToCreate;
    private final Counter closed;
    private final AtomicInteger acquiring = new AtomicInteger();
    private final Counter timedOutToAcquire;
    private final Timer totalAcquisitionTimer;
    private final Timer totalConnectionTimer;
    private final Timer totalInUseTimer;

    MicrometerConnectionPoolMetrics(String poolId, URI uri, MeterRegistry registry) {
        this(poolId, uri, registry, Tags.empty());
    }

    MicrometerConnectionPoolMetrics(String poolId, URI uri, MeterRegistry registry, Iterable<Tag> initialTags) {
        Objects.requireNonNull(poolId);
        Objects.requireNonNull(uri);
        Objects.requireNonNull(registry);

        this.id = poolId;
        var port = uri.getPort();
        if (port == -1) {
            port = BoltServerAddress.DEFAULT_PORT;
        }
        Iterable<Tag> tags = Tags.concat(initialTags, "address", String.format("%s:%d", uri.getHost(), port));

        Gauge.builder(IN_USE, this::inUse).tags(tags).register(registry);
        Gauge.builder(IDLE, this::idle).tags(tags).register(registry);
        Gauge.builder(CREATING, creating, AtomicInteger::get).tags(tags).register(registry);
        failedToCreate = Counter.builder(FAILED).tags(tags).register(registry);
        closed = Counter.builder(CLOSED).tags(tags).register(registry);
        Gauge.builder(ACQUIRING, acquiring, AtomicInteger::get).tags(tags).register(registry);
        timedOutToAcquire = Counter.builder(ACQUISITION_TIMEOUT).tags(tags).register(registry);
        totalAcquisitionTimer = Timer.builder(ACQUISITION).tags(tags).register(registry);
        totalConnectionTimer = Timer.builder(CREATION).tags(tags).register(registry);
        totalInUseTimer = Timer.builder(USAGE).tags(tags).register(registry);
    }

    public void beforeCreating() {
        creating.incrementAndGet();
    }

    public void afterFailedToCreate() {
        failedToCreate.increment();
        creating.decrementAndGet();
    }

    public void afterCreated(Timer.Sample sample) {
        creating.decrementAndGet();
        sample.stop(totalConnectionTimer);
        idle.incrementAndGet();
    }

    public void afterClosed() {
        idle.decrementAndGet();
        closed.increment();
    }

    public void beforeAcquiringOrCreating() {
        acquiring.incrementAndGet();
    }

    public void afterAcquiringOrCreating() {
        acquiring.decrementAndGet();
    }

    public void afterAcquiredOrCreated(Timer.Sample sample) {
        acquiring.decrementAndGet();
        sample.stop(totalAcquisitionTimer);
    }

    public void afterTimedOutToAcquireOrCreate() {
        timedOutToAcquire.increment();
    }

    public void onAcquired() {
        inUse.incrementAndGet();
        idle.decrementAndGet();
    }

    public void released(Timer.Sample sample) {
        inUse.decrementAndGet();
        idle.incrementAndGet();
        sample.stop(totalInUseTimer);
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public int inUse() {
        return inUse.get();
    }

    @Override
    public int idle() {
        return idle.get();
    }

    @Override
    public int creating() {
        return creating.get();
    }

    @Override
    public long created() {
        return totalConnectionTimer.count();
    }

    @Override
    public long failedToCreate() {
        return count(failedToCreate);
    }

    @Override
    public long closed() {
        return count(closed);
    }

    @Override
    public int acquiring() {
        return acquiring.get();
    }

    @Override
    public long acquired() {
        return totalAcquisitionTimer.count();
    }

    @Override
    public long timedOutToAcquire() {
        return count(timedOutToAcquire);
    }

    @Override
    public long totalAcquisitionTime() {
        return (long) totalAcquisitionTimer.totalTime(TimeUnit.MILLISECONDS);
    }

    @Override
    public long totalConnectionTime() {
        return (long) totalConnectionTimer.totalTime(TimeUnit.MILLISECONDS);
    }

    @Override
    public long totalInUseTime() {
        return (long) totalInUseTimer.totalTime(TimeUnit.MILLISECONDS);
    }

    @Override
    public long totalInUseCount() {
        return totalInUseTimer.count();
    }

    @Override
    public String toString() {
        return format(
                "%s=[created=%s, closed=%s, creating=%s, failedToCreate=%s, acquiring=%s, acquired=%s, "
                        + "timedOutToAcquire=%s, inUse=%s, idle=%s, "
                        + "totalAcquisitionTime=%s, totalConnectionTime=%s, totalInUseTime=%s, totalInUseCount=%s]",
                id(),
                created(),
                closed(),
                creating(),
                failedToCreate(),
                acquiring(),
                acquired(),
                timedOutToAcquire(),
                inUse(),
                idle(),
                totalAcquisitionTime(),
                totalConnectionTime(),
                totalInUseTime(),
                totalInUseCount());
    }

    private long count(Counter counter) {
        return (long) counter.count();
    }
}
