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
    CompletionStage<BoltConnection> route(Set<String> bookmarks, String databaseName, String impersonatedUser);

    CompletionStage<BoltConnection> beginTransaction(
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig);

    CompletionStage<BoltConnection> runInAutoCommitTransaction(
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
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

    CompletionStage<Void> flush(ResponseHandler handler);

    CompletionStage<Void> close();

    BoltConnectionState state();

    BoltConnectionInfo connectionInfo();

    Map<String, Value> authMap();

    CompletionStage<Long> latestAuthMillis();

    void setDatabase(String database);

    void setAccessMode(AccessMode mode);

    void setImpersonatedUser(String impersonatedUser);
}
