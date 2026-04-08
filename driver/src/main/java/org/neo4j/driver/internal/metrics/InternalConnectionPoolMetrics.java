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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.driver.ConnectionPoolMetrics;

public final class InternalConnectionPoolMetrics implements ConnectionPoolMetrics {
    private final AtomicInteger inUse = new AtomicInteger();
    private final AtomicInteger idle = new AtomicInteger();

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

    InternalConnectionPoolMetrics(String poolId) {
        this.id = poolId;
    }

    public void beforeCreating() {
        creating.incrementAndGet();
    }

    public void afterFailedToCreate() {
        failedToCreate.incrementAndGet();
        creating.decrementAndGet();
    }

    public void afterCreated(long duration) {
        created.incrementAndGet();
        creating.decrementAndGet();
        totalConnectionTime.addAndGet(duration);
        idle.incrementAndGet();
    }

    public void afterClosed() {
        idle.decrementAndGet();
        closed.incrementAndGet();
    }

    public void beforeAcquiringOrCreating() {
        acquiring.incrementAndGet();
    }

    public void afterAcquiringOrCreating() {
        acquiring.decrementAndGet();
    }

    public void afterAcquiredOrCreated(long duration) {
        acquiring.decrementAndGet();
        acquired.incrementAndGet();
        totalAcquisitionTime.addAndGet(duration);
    }

    public void afterTimedOutToAcquireOrCreate() {
        timedOutToAcquire.incrementAndGet();
    }

    public void onAcquired() {
        inUse.incrementAndGet();
        idle.decrementAndGet();
    }

    public void released(long duration) {
        inUse.decrementAndGet();
        idle.incrementAndGet();
        totalInUseCount.incrementAndGet();
        totalInUseTime.addAndGet(duration);
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
        return created.get();
    }

    @Override
    public long failedToCreate() {
        return failedToCreate.get();
    }

    @Override
    public long timedOutToAcquire() {
        return timedOutToAcquire.get();
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
        return totalInUseCount.get();
    }

    @Override
    public long closed() {
        return closed.get();
    }

    @Override
    public int acquiring() {
        return acquiring.get();
    }

    @Override
    public long acquired() {
        return this.acquired.get();
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
}
