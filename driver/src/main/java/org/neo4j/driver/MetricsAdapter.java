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
package org.neo4j.driver;

/**
 * Defines which metrics consumer to use: Should metrics be consumed and exposed via driver's default consumer or provided with one of the external facades.
 */
public enum MetricsAdapter {
    /**
     * Disables metrics.
     */
    DEV_NULL,

    /**
     * Consumes and publishes metrics via the driver itself.
     */
    DEFAULT,

    /**
     * Consumes and publishes metrics via Micrometer. Ensure that Micrometer is on classpath when using this option.
     */
    MICROMETER
}
