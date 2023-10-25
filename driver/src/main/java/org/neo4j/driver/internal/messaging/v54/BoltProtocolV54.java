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
package org.neo4j.driver.internal.messaging.v54;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.handlers.TelemetryResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.TelemetryMessage;
import org.neo4j.driver.internal.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.spi.Connection;

public class BoltProtocolV54 extends BoltProtocolV53 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 4);
    public static final BoltProtocol INSTANCE = new BoltProtocolV54();

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    public CompletionStage<Void> telemetry(Connection connection, Integer api) {
        var telemetry = new TelemetryMessage(api);
        var future = new CompletableFuture<Void>();
        connection.write(telemetry, new TelemetryResponseHandler(future));
        return future;
    }

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV54();
    }
}
