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
package org.neo4j.driver.internal.bolt.api;

/**
 * An enum of valid telemetry metrics.
 */
public enum TelemetryApi {
    MANAGED_TRANSACTION(0),
    UNMANAGED_TRANSACTION(1),
    AUTO_COMMIT_TRANSACTION(2),
    EXECUTABLE_QUERY(3);

    private final Integer value;

    TelemetryApi(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
