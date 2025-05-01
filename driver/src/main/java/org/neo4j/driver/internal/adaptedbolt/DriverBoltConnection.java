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
package org.neo4j.driver.internal.adaptedbolt;

import java.util.List;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.driver.internal.value.BoltValueFactory;

public interface DriverBoltConnection {
    default CompletionStage<Void> writeAndFlush(DriverResponseHandler handler, Message messages) {
        return writeAndFlush(handler, List.of(messages));
    }

    CompletionStage<Void> writeAndFlush(DriverResponseHandler handler, List<Message> messages);

    CompletionStage<Void> write(List<Message> messages);

    CompletionStage<Void> forceClose(String reason);

    CompletionStage<Void> close();

    // ----- MUTABLE DATA -----

    CompletionStage<AuthInfo> authData();

    // ----- IMMUTABLE DATA -----

    String serverAgent();

    BoltServerAddress serverAddress();

    BoltProtocolVersion protocolVersion();

    boolean telemetrySupported();

    boolean serverSideRoutingEnabled();

    // ----- EXTRAS -----

    BoltValueFactory valueFactory();
}
