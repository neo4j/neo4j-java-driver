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

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;

public final class SingleRoutedBoltConnectionSource implements BoltConnectionSource<RoutedBoltConnectionParameters> {
    private final BoltConnectionSource<BoltConnectionParameters> delegate;

    public SingleRoutedBoltConnectionSource(BoltConnectionSource<BoltConnectionParameters> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public CompletionStage<BoltConnection> getConnection() {
        return delegate.getConnection();
    }

    @Override
    public CompletionStage<BoltConnection> getConnection(RoutedBoltConnectionParameters parameters) {
        return delegate.getConnection(parameters).whenComplete((connection, throwable) -> {
            if (throwable == null) {
                parameters.databaseNameListener().accept(parameters.databaseName());
            }
        });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return delegate.verifyConnectivity();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return delegate.supportsMultiDb();
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return delegate.supportsSessionAuth();
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }
}
