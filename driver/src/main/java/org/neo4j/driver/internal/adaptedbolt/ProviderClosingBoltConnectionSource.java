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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;

public record ProviderClosingBoltConnectionSource(
        BoltConnectionSource<RoutedBoltConnectionParameters> delegate, BoltConnectionProvider provider)
        implements BoltConnectionSource<RoutedBoltConnectionParameters> {

    public ProviderClosingBoltConnectionSource {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(provider);
    }

    @Override
    public CompletionStage<BoltConnection> getConnection() {
        return delegate.getConnection();
    }

    @Override
    public CompletionStage<BoltConnection> getConnection(RoutedBoltConnectionParameters parameters) {
        return delegate.getConnection(parameters);
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
        return delegate.close()
                .handle((ignored, throwable) -> {
                    if (throwable != null) {
                        return provider.close()
                                .handle((closedResult, closeThrowable) -> {
                                    if (closeThrowable != null) {
                                        throwable.addSuppressed(closeThrowable);
                                    }
                                    return CompletableFuture.<Void>failedStage(throwable);
                                })
                                .thenCompose(Function.identity());
                    } else {
                        return provider.close();
                    }
                })
                .thenCompose(Function.identity());
    }
}
