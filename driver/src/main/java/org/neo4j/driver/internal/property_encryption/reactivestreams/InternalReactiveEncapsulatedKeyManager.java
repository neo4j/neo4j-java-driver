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
package org.neo4j.driver.internal.property_encryption.reactivestreams;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreams;

import java.util.Objects;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.property_encryption.AbstractEncapsulatedKeyManager;
import org.neo4j.driver.internal.property_encryption.KeyCache;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.reactivestreams.ReactiveEncapsulatedKeyManager;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

final class InternalReactiveEncapsulatedKeyManager extends AbstractEncapsulatedKeyManager
        implements ReactiveEncapsulatedKeyManager {
    private final DriverObservationProvider observationProvider;

    public InternalReactiveEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            EncapsulatedKeyRecordRepository keyRepository,
            KeyCache keyCache,
            DriverObservationProvider observationProvider) {
        super(keyEncapsulationService, keyRepository, keyCache, observationProvider);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public Publisher<EncapsulatedKey> create(String alias, KeyEncapsulationOptions encapsulationOptions) {
        var createObservation = observationProvider.createEncapsulatedKey(ReactiveEncapsulatedKeyManager.class, alias);
        return observeStreams(
                createObservation, Mono.fromCompletionStage(() -> createAsync(alias, encapsulationOptions)));
    }

    @Override
    public Publisher<EncapsulatedKey> findByAlias(String alias) {
        var findObservation =
                observationProvider.findEncapsulatedKeyByAlias(ReactiveEncapsulatedKeyManager.class, alias);
        return observeStreams(findObservation, Mono.fromCompletionStage(() -> findByAliasAsync(alias)));
    }

    @Override
    public Publisher<Void> setAliasById(String id, String alias) {
        var setObservation =
                observationProvider.setEncapsulatedKeyAlias(ReactiveEncapsulatedKeyManager.class, id, alias);
        return observeStreams(setObservation, Mono.fromCompletionStage(() -> setAliasByIdAsync(id, alias)));
    }

    @Override
    public Publisher<Void> deleteById(String id) {
        var deleteObservation = observationProvider.deleteEncapsulatedKey(ReactiveEncapsulatedKeyManager.class, id);
        return observeStreams(deleteObservation, Mono.fromCompletionStage(() -> deleteByIdAsync(id)));
    }
}
