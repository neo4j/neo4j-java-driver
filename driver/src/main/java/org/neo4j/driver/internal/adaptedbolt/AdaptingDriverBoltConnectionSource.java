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

import static org.neo4j.driver.internal.observation.util.ObservationUtil.scoped;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.internal.value.BoltValueFactory;

public class AdaptingDriverBoltConnectionSource implements DriverBoltConnectionSource {
    private final BoltConnectionSource<RoutedBoltConnectionParameters> delegate;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;
    private final boolean routed;
    private final DriverObservationProvider observationProvider;

    public AdaptingDriverBoltConnectionSource(
            BoltConnectionSource<RoutedBoltConnectionParameters> delegate,
            ErrorMapper errorMapper,
            BoltValueFactory boltValueFactory,
            boolean routed,
            DriverObservationProvider observationProvider) {
        this.delegate = Objects.requireNonNull(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
        this.routed = routed;
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<DriverBoltConnection> getConnection(
            RoutedBoltConnectionParameters parameters, Observation parentObservation) {
        return scoped(observationProvider, parentObservation, () -> delegate.getConnection(parameters))
                .exceptionally(errorMapper::mapAndThrow)
                .thenApply(boltConnection -> new AdaptingDriverBoltConnection(
                        boltConnection,
                        routed || boltConnection.serverSideRoutingEnabled()
                                ? new RoutedErrorMapper(boltConnection.serverAddress(), parameters.accessMode())
                                : errorMapper,
                        boltValueFactory,
                        observationProvider));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return delegate.verifyConnectivity().exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return delegate.supportsMultiDb().exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return delegate.supportsSessionAuth().exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close().exceptionally(errorMapper::mapAndThrow);
    }
}
