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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.driver.util.Preview;

/**
 * @since 6.0.0
 */
@Preview(name = "Observability")
public final class ConnectionPoolMetricsHandler implements ObservationHandler<Observation.Context> {
    private static final String COUNT_METRIC_NAME = "db.client.connection.count";
    private static final String POOL_TAG_NAME = "db.client.connection.pool.name";
    private static final String STATE_TAG_NAME = "db.client.connection.state";
    private static final String IDLE = "idle";
    private static final String USED = "used";
    private static final String AQUIRING_METRIC_NAME = "db.client.connection.pending.requests";
    private static final String CREATING_METRIC_NAME = "neo4j.db.client.connection.creating.requests";
    private static final String DB_SYSTEM_TAG_NAME = "db.system.name";
    private static final String MAX_SIZE_METRIC_NAME = "db.client.connection.max";
    private static final String TIMEOUTS_METRIC_NAME = "db.client.connection.timeouts";

    private final MeterRegistry meterRegistry;
    private final Map<String, ConnectionPoolMetrics> idToMetrics;

    public ConnectionPoolMetricsHandler(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.idToMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof AbstractConnectionPoolContext;
    }

    @Override
    public void onStart(Observation.Context context) {
        try {
            if (context instanceof AbstractConnectionPoolContext poolContext) {
                var metrics = idToMetrics.get(poolContext.id());
                if (metrics == null) {
                    return;
                }
                if (context instanceof PooledConnectionAcquireContext) {
                    metrics.onAcquiring();
                } else if (context instanceof PooledConnectionCreateContext) {
                    metrics.onCreating();
                } else if (context instanceof PooledConnectionInUseContext) {
                    metrics.onInUseStart();
                } else if (context instanceof PooledConnectionCloseContext) {
                    metrics.onClosing();
                }
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onStop(Observation.Context context) {
        try {
            if (context instanceof ConnectionPoolCreateContext poolCreateContext) {
                if (context.getError() != null) {
                    return;
                }
                idToMetrics.computeIfAbsent(
                        poolCreateContext.id(),
                        id -> new ConnectionPoolMetrics(
                                meterRegistry, id, poolCreateContext.uri(), poolCreateContext.maxSize()));
            } else if (context instanceof AbstractConnectionPoolContext poolContext) {
                var metrics = idToMetrics.get(poolContext.id());
                if (metrics == null) {
                    return;
                }
                if (context instanceof ConnectionPoolCloseContext) {
                    metrics.close();
                } else if (context instanceof PooledConnectionAcquireContext) {
                    var error = context.getError();
                    if (error != null) {
                        if (error instanceof TimeoutException) {
                            metrics.onTimeout();
                        }
                        return;
                    }
                    metrics.onAcquired();
                } else if (context instanceof PooledConnectionCreateContext) {
                    if (context.getError() != null) {
                        metrics.onFailedToCreate();
                    } else {
                        metrics.onCreated();
                    }
                } else if (context instanceof PooledConnectionInUseContext) {
                    metrics.onInUseStop();
                }
            }
        } catch (Exception ignored) {
        }
    }

    private static final class ConnectionPoolMetrics {
        private final MeterRegistry meterRegistry;
        private final AtomicInteger inUse;
        private final AtomicInteger idle;
        private final AtomicInteger creating;
        private final AtomicInteger acquiring;
        private final AtomicInteger maxSize;

        private final Gauge inUseGauge;
        private final Gauge idleGauge;
        private final Gauge creatingGauge;
        private final Gauge acquiringGauge;
        private final Gauge maxSizeGauge;
        private final Counter timeouts;

        ConnectionPoolMetrics(MeterRegistry meterRegistry, String id, URI uri, int maxSize) {
            this.meterRegistry = Objects.requireNonNull(meterRegistry);

            this.inUse = new AtomicInteger(0);
            this.idle = new AtomicInteger(0);
            this.creating = new AtomicInteger(0);
            this.acquiring = new AtomicInteger(0);
            this.maxSize = new AtomicInteger(maxSize);

            var baseTags = Tags.of(DB_SYSTEM_TAG_NAME, KeyValuesUtil.DB_SYSTEM_NAME, POOL_TAG_NAME, id);

            this.inUseGauge = Gauge.builder(COUNT_METRIC_NAME, this.inUse, AtomicInteger::get)
                    .tags(Tags.concat(baseTags, STATE_TAG_NAME, USED))
                    .register(meterRegistry);
            this.idleGauge = Gauge.builder(COUNT_METRIC_NAME, this.idle, AtomicInteger::get)
                    .tags(Tags.concat(baseTags, STATE_TAG_NAME, IDLE))
                    .register(meterRegistry);
            this.creatingGauge = Gauge.builder(CREATING_METRIC_NAME, this.creating, AtomicInteger::get)
                    .tags(baseTags)
                    .register(meterRegistry);
            this.acquiringGauge = Gauge.builder(AQUIRING_METRIC_NAME, this.acquiring, AtomicInteger::get)
                    .tags(baseTags)
                    .register(meterRegistry);
            this.maxSizeGauge = Gauge.builder(MAX_SIZE_METRIC_NAME, this.maxSize, AtomicInteger::get)
                    .tags(baseTags)
                    .register(meterRegistry);
            this.timeouts = Counter.builder(TIMEOUTS_METRIC_NAME).tags(baseTags).register(meterRegistry);
        }

        void onAcquiring() {
            acquiring.incrementAndGet();
        }

        void onTimeout() {
            this.timeouts.increment();
        }

        void onAcquired() {
            acquiring.decrementAndGet();
        }

        void onCreating() {
            creating.incrementAndGet();
        }

        void onFailedToCreate() {
            creating.decrementAndGet();
        }

        void onCreated() {
            creating.decrementAndGet();
            idle.incrementAndGet();
        }

        void onInUseStart() {
            inUse.incrementAndGet();
            idle.decrementAndGet();
        }

        void onInUseStop() {
            inUse.decrementAndGet();
            idle.incrementAndGet();
        }

        void onClosing() {
            idle.decrementAndGet();
        }

        void close() {
            meterRegistry.remove(inUseGauge);
            meterRegistry.remove(idleGauge);
            meterRegistry.remove(creatingGauge);
            meterRegistry.remove(acquiringGauge);
            meterRegistry.remove(maxSizeGauge);
            meterRegistry.remove(timeouts);
        }
    }
}
