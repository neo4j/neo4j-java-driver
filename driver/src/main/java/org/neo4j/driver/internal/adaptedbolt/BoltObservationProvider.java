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

import java.net.URI;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.observation.BoltExchangeObservation;
import org.neo4j.bolt.connection.observation.HttpExchangeObservation;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.pooled.observation.PoolObservationProvider;
import org.neo4j.driver.internal.observation.DriverObservationProvider;

public final class BoltObservationProvider implements PoolObservationProvider {
    private final DriverObservationProvider delegate;

    public BoltObservationProvider(DriverObservationProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Observation connectionPoolCreate(String id, URI uri, int maxSize) {
        return new BoltObservation(
                delegate.connectionPoolCreate(id, uri, maxSize).start());
    }

    @Override
    public Observation connectionPoolClose(String id, URI uri) {
        return new BoltObservation(delegate.connectionPoolClose(id, uri).start());
    }

    @Override
    public Observation pooledConnectionCreate(String id, URI uri) {
        return new BoltObservation(delegate.pooledConnectionCreate(id, uri).start());
    }

    @Override
    public Observation pooledConnectionClose(String id, URI uri) {
        return new BoltObservation(delegate.pooledConnectionClose(id, uri).start());
    }

    @Override
    public Observation pooledConnectionAcquire(String id, URI uri) {
        return new BoltObservation(delegate.pooledConnectionAcquire(id, uri).start());
    }

    @Override
    public Observation pooledConnectionInUse(ImmutableObservation parentObsevation, String id, URI uri) {
        return supplyInScope(
                parentObsevation,
                () -> new BoltObservation(
                        delegate.pooledConnectionInUse(id, uri).start()));
    }

    @Override
    public BoltExchangeObservation boltExchange(
            ImmutableObservation parentObsevation,
            String host,
            int port,
            BoltProtocolVersion boltVersion,
            BiConsumer<String, String> setter) {
        return NoopBoltExchangeObservationImpl.getInstance();
    }

    @Override
    public HttpExchangeObservation httpExchange(
            ImmutableObservation parentObsevation,
            URI uri,
            String method,
            String uriTemplate,
            BiConsumer<String, String> setter) {
        return NoopHttpExchangeObservationImpl.getInstance();
    }

    @Override
    public Observation scopedObservation() {
        var observation = delegate.scopedObservation();
        return observation != null ? new BoltObservation(observation) : null;
    }

    @Override
    public <T> T supplyInScope(ImmutableObservation observation, Supplier<T> supplier) {
        if (observation instanceof BoltObservation boltObservation) {
            var scopedObservation = scopedObservation();
            if (observation.equals(scopedObservation)) {
                return supplier.get();
            } else {
                try (var scope = boltObservation.delegate().openScope()) {
                    return supplier.get();
                }
            }
        } else {
            return supplier.get();
        }
    }
}
