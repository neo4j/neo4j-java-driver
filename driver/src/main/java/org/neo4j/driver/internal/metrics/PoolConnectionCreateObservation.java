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

import java.time.Clock;
import java.util.Objects;
import org.neo4j.driver.internal.observation.Observation;

final class PoolConnectionCreateObservation extends AbstractObservation {
    private final InternalConnectionPoolMetrics metrics;
    private final Clock clock;
    private volatile long start;

    PoolConnectionCreateObservation(InternalConnectionPoolMetrics metrics, Clock clock) {
        this.metrics = Objects.requireNonNull(metrics);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public Observation start() {
        metrics.beforeCreating();
        start = clock.millis();
        return this;
    }

    @Override
    public Observation error(Throwable error) {
        metrics.afterFailedToCreate();
        return this;
    }

    @Override
    public void stop() {
        var duration = clock.millis() - start;
        metrics.afterCreated(duration);
    }
}
