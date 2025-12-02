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
package org.neo4j.driver.internal.encryption;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.encryption.EncapsulatedKey;
import org.neo4j.driver.encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationService;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.PropertyEncryptionException;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.util.Futures;

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
        return observeAsync(encapsulateObservation, () -> {
                    try {
                        return keyEncapsulationService.encapsulate(encapsulationOptions);
                    } catch (Neo4jException neo4jException) {
                        throw neo4jException;
                    } catch (Exception exception) {
                        throw new PropertyEncryptionException("Error encapsulating key", exception);
                    }
                })
                .exceptionally(throwable -> {
                    throwable = Futures.completionExceptionCause(throwable);
                    if (throwable instanceof Neo4jException neo4jException) {
                        throw neo4jException;
                    } else {
                        throw new PropertyEncryptionException("Error encapsulating key", throwable);
                    }
                })
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
        Objects.requireNonNull(alias);
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
