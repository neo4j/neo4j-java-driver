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
package org.neo4j.driver.internal.boltlistener;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.observation.ImmutableObservation;

final class ListeningBoltConnectionProvider implements BoltConnectionProvider {
    private final BoltConnectionProvider delegate;
    private final BoltConnectionListener boltConnectionListener;

    public ListeningBoltConnectionProvider(
            BoltConnectionProvider delegate, BoltConnectionListener boltConnectionListener) {
        this.delegate = Objects.requireNonNull(delegate);
        this.boltConnectionListener = Objects.requireNonNull(boltConnectionListener);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            URI uri,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            ImmutableObservation parentObservation) {
        return delegate.connect(
                        uri,
                        routingContextAddress,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        authToken,
                        minVersion,
                        notificationConfig,
                        parentObservation)
                .thenApply(boltConnection -> {
                    boltConnection = new ListeningBoltConnection(boltConnection, boltConnectionListener);
                    boltConnectionListener.onOpen(boltConnection);
                    return boltConnection;
                });
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }
}
