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

import static org.neo4j.driver.internal.util.Futures.futureCompletingConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;

public class ApiTelemetryWork {
    private final TelemetryApi telemetryApi;
    private final AtomicBoolean completedWithSuccess;

    private final AtomicBoolean enabled;

    public ApiTelemetryWork(TelemetryApi telemetryApi) {
        this.telemetryApi = telemetryApi;
        this.completedWithSuccess = new AtomicBoolean(false);
        this.enabled = new AtomicBoolean(true);
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public CompletionStage<Void> execute(Connection connection, BoltProtocol protocol) {
        var future = new CompletableFuture<Void>();
        if (connection.isTelemetryEnabled() && enabled.get() && !this.completedWithSuccess.get()) {
            protocol.telemetry(connection, telemetryApi.getValue())
                    .thenAccept((unused) -> completedWithSuccess.set(true))
                    .whenComplete(futureCompletingConsumer(future));
        } else {
            future.complete(null);
        }
        return future;
    }
}
