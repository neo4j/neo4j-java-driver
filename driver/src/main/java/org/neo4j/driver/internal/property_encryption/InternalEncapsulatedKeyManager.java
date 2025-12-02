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
package org.neo4j.driver.internal.property_encryption;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observe;

import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

final class InternalEncapsulatedKeyManager extends AbstractEncapsulatedKeyManager implements EncapsulatedKeyManager {
    private final DriverObservationProvider observationProvider;

    InternalEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            EncapsulatedKeyRecordRepository keyRepository,
            KeyCache keyCache,
            DriverObservationProvider observationProvider) {
        super(keyEncapsulationService, keyRepository, keyCache, observationProvider);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public EncapsulatedKey create(String alias, KeyEncapsulationOptions encapsulationOptions) {
        var createObservation = observationProvider.createEncapsulatedKey(EncapsulatedKeyManager.class, alias);
        return observe(createObservation, () -> createAsync(alias, encapsulationOptions)
                .toCompletableFuture()
                .join());
    }

    @Override
    public Optional<EncapsulatedKey> findByAlias(String alias) {
        var findObservation = observationProvider.findEncapsulatedKeyByAlias(EncapsulatedKeyManager.class, alias);
        return observe(
                findObservation,
                () -> Optional.ofNullable(
                        findByAliasAsync(alias).toCompletableFuture().join()));
    }

    @Override
    public void setAliasById(String id, String alias) {
        var setObservation = observationProvider.setEncapsulatedKeyAlias(EncapsulatedKeyManager.class, id, alias);
        observe(
                setObservation,
                () -> setAliasByIdAsync(id, alias).toCompletableFuture().join());
    }

    @Override
    public void deleteById(String id) {
        var deleteObservation = observationProvider.deleteEncapsulatedKey(EncapsulatedKeyManager.class, id);
        observe(
                deleteObservation,
                () -> deleteByIdAsync(id).toCompletableFuture().join());
    }
}
