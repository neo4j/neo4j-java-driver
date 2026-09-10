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
package org.neo4j.driver.internal.property_encryption.async;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.property_encryption.AbstractEncapsulatedKeyManager;
import org.neo4j.driver.internal.property_encryption.KeyCache;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.async.AsyncEncapsulatedKeyManager;

final class InternalAsyncEncapsulatedKeyManager extends AbstractEncapsulatedKeyManager
        implements AsyncEncapsulatedKeyManager {
    private final DriverObservationProvider observationProvider;

    public InternalAsyncEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            EncapsulatedKeyRecordRepository keyRepository,
            KeyCache keyCache,
            DriverObservationProvider observationProvider) {
        super(keyEncapsulationService, keyRepository, keyCache, observationProvider);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<EncapsulatedKey> createAsync(String alias, KeyEncapsulationOptions encapsulationOptions) {
        var createObservation = observationProvider.createEncapsulatedKey(AsyncEncapsulatedKeyManager.class, alias);
        return observeAsync(createObservation, () -> super.createAsync(alias, encapsulationOptions));
    }

    @Override
    public CompletionStage<EncapsulatedKey> findByAliasAsync(String alias) {
        var findObservation = observationProvider.findEncapsulatedKeyByAlias(AsyncEncapsulatedKeyManager.class, alias);
        return observeAsync(findObservation, () -> super.findByAliasAsync(alias));
    }

    @Override
    public CompletionStage<Void> setAliasByIdAsync(String id, String alias) {
        var setObservation = observationProvider.setEncapsulatedKeyAlias(AsyncEncapsulatedKeyManager.class, id, alias);
        return observeAsync(setObservation, () -> super.setAliasByIdAsync(id, alias));
    }

    @Override
    public CompletionStage<Void> deleteByIdAsync(String id) {
        var deleteObservation = observationProvider.deleteEncapsulatedKey(AsyncEncapsulatedKeyManager.class, id);
        return observeAsync(deleteObservation, () -> super.deleteByIdAsync(id));
    }
}
