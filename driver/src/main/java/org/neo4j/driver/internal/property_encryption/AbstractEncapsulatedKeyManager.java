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

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public abstract class AbstractEncapsulatedKeyManager {
    private final KeyEncapsulationService keyEncapsulationService;
    private final EncapsulatedKeyRecordRepository keyRepository;
    private final KeyCache keyCache;
    private final DriverObservationProvider observationProvider;

    public AbstractEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            EncapsulatedKeyRecordRepository keyRepository,
            KeyCache keyCache,
            DriverObservationProvider observationProvider) {
        this.keyEncapsulationService = Objects.requireNonNull(keyEncapsulationService);
        this.keyRepository = Objects.requireNonNull(keyRepository);
        this.keyCache = Objects.requireNonNull(keyCache);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    public CompletionStage<EncapsulatedKey> createAsync(String alias, KeyEncapsulationOptions encapsulationOptions) {
        var encapsulateObservation = observationProvider.keyEncapsulationServiceEncapsulate();
        return observeAsync(encapsulateObservation, () -> keyEncapsulationService.encapsulate(encapsulationOptions))
                .thenCompose(encapsulationResult -> {
                    var createObservation = observationProvider.encapsulatedKeyRepositoryCreate();
                    return observeAsync(
                                    createObservation,
                                    () -> keyRepository.create(
                                            alias, encapsulationResult.encapsulation(), encapsulationResult.metadata()))
                            .thenApply(encapsulatedKeyRecord -> {
                                keyCache.create(encapsulatedKeyRecord.id(), alias, encapsulationResult.key());
                                return encapsulatedKeyRecord;
                            });
                })
                .thenApply(encapsulatedKey -> new EncapsulatedKeyRecord(
                        encapsulatedKey.id(), encapsulatedKey.alias().orElse(null)));
    }

    public CompletionStage<EncapsulatedKey> findByAliasAsync(String alias) {
        if (alias == null) {
            return CompletableFuture.failedStage(new NullPointerException("alias must not be null"));
        }
        var cachedKey = keyCache.findByAlias(alias);
        if (cachedKey != null) {
            return CompletableFuture.completedStage(new EncapsulatedKeyRecord(cachedKey.id(), alias));
        }
        var findObservation = observationProvider.encapsulatedKeyRepositoryFindByAlias();
        return observeAsync(findObservation, () -> keyRepository.findByAlias(alias))
                .thenApply(encapsulatedKey -> {
                    if (encapsulatedKey == null) {
                        return null;
                    }
                    return new EncapsulatedKeyRecord(
                            encapsulatedKey.id(), encapsulatedKey.alias().orElse(null));
                });
    }

    public CompletionStage<Void> setAliasByIdAsync(String id, String alias) {
        Objects.requireNonNull(id);
        var setObservation = observationProvider.encapsulatedKeyRepositorySetAliasById();
        return observeAsync(setObservation, () -> keyRepository.setAliasById(id, alias))
                .thenAccept(ignored -> keyCache.setAlias(id, alias));
    }

    public CompletionStage<Void> deleteByIdAsync(String id) {
        var deleteObservation = observationProvider.encapsulatedKeyRepositoryDeleteById();
        return observeAsync(deleteObservation, () -> keyRepository.deleteById(id))
                .thenAccept(ignored -> keyCache.deleteById(id));
    }

    private record EncapsulatedKeyRecord(String id, String aliasRef) implements EncapsulatedKey {
        @Override
        public Optional<String> alias() {
            return Optional.ofNullable(aliasRef);
        }
    }
}
