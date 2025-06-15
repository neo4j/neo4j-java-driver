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
package org.neo4j.driver.observation.metrics.internal;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.observation.ObservationProvider;
import org.neo4j.driver.observation.metrics.ConnectionPoolMetrics;
import org.neo4j.driver.observation.metrics.Metrics;

public final class InternalMetrics implements Metrics {
    private final Map<String, ConnectionPoolMetrics> connectionPoolMetrics;
    private ObservationProvider observationProvider;

    public InternalMetrics() {
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
    }

    public void registerPoolMetrics(String id) {
        this.connectionPoolMetrics.put(id, new InternalConnectionPoolMetrics(id));
    }

    public void deregisterPoolMetrics(String id) {
        this.connectionPoolMetrics.remove(id);
    }

    public InternalConnectionPoolMetrics getConnectionPoolMetrics(String id) {
        return (InternalConnectionPoolMetrics) connectionPoolMetrics.get(id);
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics() {
        return unmodifiableCollection(this.connectionPoolMetrics.values());
    }

    @Override
    public String toString() {
        return format("PoolMetrics=%s", connectionPoolMetrics);
    }
}
