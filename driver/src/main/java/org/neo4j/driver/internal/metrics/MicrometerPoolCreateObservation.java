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

import java.net.URI;
import java.util.Objects;

final class MicrometerPoolCreateObservation extends AbstractObservation {
    private final MicrometerMetrics metrics;
    private final String id;
    private final URI uri;

    MicrometerPoolCreateObservation(MicrometerMetrics metrics, String id, URI uri) {
        this.metrics = Objects.requireNonNull(metrics);
        this.id = Objects.requireNonNull(id);
        this.uri = Objects.requireNonNull(uri);
    }

    @Override
    public void stop() {
        metrics.registerPoolMetrics(id, uri);
    }
}
