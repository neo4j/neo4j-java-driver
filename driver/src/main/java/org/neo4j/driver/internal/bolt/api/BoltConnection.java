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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;

public interface BoltConnection {
    CompletionStage<BoltConnection> route(DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks);

    CompletionStage<BoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig);

    CompletionStage<BoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig);

    CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters);

    CompletionStage<BoltConnection> pull(long qid, long request);

    CompletionStage<BoltConnection> discard(long qid, long number);

    CompletionStage<BoltConnection> commit();

    CompletionStage<BoltConnection> rollback();

    CompletionStage<BoltConnection> reset();

    CompletionStage<BoltConnection> logoff();

    CompletionStage<BoltConnection> logon(Map<String, Value> authMap);

    CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi);

    CompletionStage<BoltConnection> clear();

    CompletionStage<Void> flush(ResponseHandler handler);

    CompletionStage<Void> forceClose(String reason);

    CompletionStage<Void> close();

    // ----- MUTABLE DATA -----

    BoltConnectionState state();

    CompletionStage<AuthData> authData();

    // ----- IMMUTABLE DATA -----

    String serverAgent();

    BoltServerAddress serverAddress();

    BoltProtocolVersion protocolVersion();

    boolean telemetrySupported();
}
