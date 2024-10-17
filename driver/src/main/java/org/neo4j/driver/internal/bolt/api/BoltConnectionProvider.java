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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Value;

public interface BoltConnectionProvider {
    CompletionStage<Void> init(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener);

    CompletionStage<BoltConnection> connect(
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer);

    CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap);

    CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap);

    CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap);

    CompletionStage<Void> close();
}
