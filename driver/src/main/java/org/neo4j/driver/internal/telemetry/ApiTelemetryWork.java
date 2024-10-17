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
package org.neo4j.driver.internal.telemetry;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;

public record ApiTelemetryWork(TelemetryApi telemetryApi, AtomicBoolean enabled, AtomicBoolean acknowledged) {
    public ApiTelemetryWork(TelemetryApi telemetryApi) {
        this(telemetryApi, new AtomicBoolean(), new AtomicBoolean());
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public void acknowledge() {
        this.acknowledged.set(true);
    }

    public CompletionStage<BoltConnection> pipelineTelemetryIfEnabled(BoltConnection connection) {
        if (enabled.get() && connection.telemetrySupported() && !(acknowledged.get())) {
            return connection.telemetry(telemetryApi);
        } else {
            return CompletableFuture.completedStage(connection);
        }
    }

    // for testing
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (ApiTelemetryWork) o;
        return Objects.equals(enabled.get(), that.enabled.get())
                && telemetryApi == that.telemetryApi
                && Objects.equals(acknowledged.get(), that.acknowledged.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(telemetryApi, enabled.get(), acknowledged.get());
    }
}
