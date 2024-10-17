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
package org.neo4j.driver.internal;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.async.NetworkSession;

public interface SessionFactory {
    NetworkSession newInstance(
            SessionConfig sessionConfig,
            NotificationConfig notificationConfig,
            AuthToken overrideAuthToken,
            boolean telemetryDisabled);

    CompletionStage<Void> verifyConnectivity();

    CompletionStage<Void> close();

    CompletionStage<Boolean> supportsMultiDb();

    CompletionStage<Boolean> supportsSessionAuth();
}
