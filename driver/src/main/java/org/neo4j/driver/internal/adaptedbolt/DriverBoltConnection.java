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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.driver.Value;

public interface DriverBoltConnection {
    <T> CompletionStage<T> onLoop(Supplier<T> supplier);

    CompletionStage<DriverBoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks);

    CompletionStage<DriverBoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig);

    CompletionStage<DriverBoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig);

    CompletionStage<DriverBoltConnection> run(String query, Map<String, Value> parameters);

    CompletionStage<DriverBoltConnection> pull(long qid, long request);

    CompletionStage<DriverBoltConnection> discard(long qid, long number);

    CompletionStage<DriverBoltConnection> commit();

    CompletionStage<DriverBoltConnection> rollback();

    CompletionStage<DriverBoltConnection> reset();

    CompletionStage<DriverBoltConnection> logoff();

    CompletionStage<DriverBoltConnection> logon(Map<String, Value> authMap);

    CompletionStage<DriverBoltConnection> telemetry(TelemetryApi telemetryApi);

    CompletionStage<DriverBoltConnection> clear();

    CompletionStage<Void> flush(DriverResponseHandler handler);

    CompletionStage<Void> forceClose(String reason);

    CompletionStage<Void> close();

    // ----- MUTABLE DATA -----

    BoltConnectionState state();

    CompletionStage<AuthInfo> authData();

    // ----- IMMUTABLE DATA -----

    String serverAgent();

    BoltServerAddress serverAddress();

    BoltProtocolVersion protocolVersion();

    boolean telemetrySupported();

    boolean serverSideRoutingEnabled();
}
