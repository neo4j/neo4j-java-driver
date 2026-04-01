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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.neo4j.driver.internal.observation.Observation;

final class MicrometerPoolConnectionAcquireObservation extends AbstractObservation {
    private final MicrometerConnectionPoolMetrics metrics;
    private final MeterRegistry meterRegistry;
    private volatile Timer.Sample sample;
    private volatile boolean failed;

    MicrometerPoolConnectionAcquireObservation(MicrometerConnectionPoolMetrics metrics, MeterRegistry meterRegistry) {
        this.metrics = Objects.requireNonNull(metrics);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
    }

    @Override
    public Observation start() {
        metrics.beforeAcquiringOrCreating();
        sample = Timer.start(meterRegistry);
        return this;
    }

    @Override
    public Observation error(Throwable error) {
        failed = true;
        if (error instanceof TimeoutException) {
            metrics.afterTimedOutToAcquireOrCreate();
        }
        return this;
    }

    @Override
    public void stop() {
        if (failed) {
            metrics.afterAcquiringOrCreating();
        } else {
            metrics.afterAcquiredOrCreated(sample);
        }
    }
}
