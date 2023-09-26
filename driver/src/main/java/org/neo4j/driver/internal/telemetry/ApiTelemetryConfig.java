/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal.telemetry;

import java.util.Objects;

/**
 * Holds the configuration of the Api Telemetry.
 */
public class ApiTelemetryConfig {
    private final TelemetryApi telemetryApi;
    private final boolean enabled;

    private ApiTelemetryConfig(TelemetryApi telemetryApi, boolean enabled) {
        this.telemetryApi = telemetryApi;
        this.enabled = enabled;
    }

    public TelemetryApi getTelemetryApi() {
        return telemetryApi;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public static ApiTelemetryConfig disabled() {
        return new ApiTelemetryConfig(TelemetryApi.MANAGED_TRANSACTION, false);
    }

    public static ApiTelemetryConfig ofApi(TelemetryApi api) {
        return new ApiTelemetryConfig(api, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ApiTelemetryConfig that = (ApiTelemetryConfig) o;
        return enabled == that.enabled && telemetryApi == that.telemetryApi;
    }

    @Override
    public int hashCode() {
        return Objects.hash(telemetryApi, enabled);
    }
}
