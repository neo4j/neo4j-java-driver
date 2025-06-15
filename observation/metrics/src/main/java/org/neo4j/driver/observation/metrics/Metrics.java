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
package org.neo4j.driver.observation.metrics;

import java.util.Collection;
import org.neo4j.driver.observation.metrics.internal.InternalMetrics;
import org.neo4j.driver.util.Preview;

/**
 * Provides driver internal metrics.
 * @since 6.0.0
 */
@Preview(name = "Observability")
public sealed interface Metrics permits InternalMetrics {
    /**
     * Connection pool metrics records metrics of connection pools that are currently in use.
     * As the connection pools are dynamically added and removed while the server topology changes, the metrics collection changes over time.
     * @return Connection pool metrics for all current active pools.
     */
    Collection<ConnectionPoolMetrics> connectionPoolMetrics();
}
