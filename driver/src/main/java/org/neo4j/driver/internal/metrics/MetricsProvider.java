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

import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.driver.Metrics;

/**
 * An adapter that collects driver metrics via {@link MetricsListener} and publishes them via {@link Metrics} instance.
 */
public interface MetricsProvider {
    /**
     * @return The actual metrics type to use
     */
    Metrics metrics();

    /**
     * @return A listener that will be notified on certain events so that it can collect metrics about them.
     */
    MetricsListener metricsListener();
}
