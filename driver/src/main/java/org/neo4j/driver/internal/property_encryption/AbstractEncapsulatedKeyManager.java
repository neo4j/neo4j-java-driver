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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;

public abstract class AbstractEncapsulatedKeyManager implements BaseEncapsulatedKeyManager {
    private final KeyEncapsulationService keyEncapsulationService;
    private final PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository;

    public AbstractEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository) {
        this.keyEncapsulationService = keyEncapsulationService;
        this.keyRepository = keyRepository;
    }

    public CompletionStage<EncapsulatedKey> createAsync(String alias, KeyEncapsulationOptions encapsulationOptions) {
        return keyEncapsulationService
                .encapsulate(encapsulationOptions)
                .thenCompose(encapsulationResult -> keyRepository.save(
                        alias,
                        encapsulationResult.encapsulation(),
                        encapsulationResult.options().toMap()))
                .thenApply(encapsulatedKey -> new EncapsulatedKeyRecord(
                        encapsulatedKey.id(), encapsulatedKey.alias().orElse(null)));
    }

    public CompletionStage<EncapsulatedKey> findByAliasAsync(String alias) {
        if (alias == null) {
            return CompletableFuture.failedStage(new NullPointerException("alias must not be null"));
        }
        return keyRepository
                .findByAlias(alias)
                .thenApply(encapsulatedKey -> new EncapsulatedKeyRecord(
                        encapsulatedKey.id(), encapsulatedKey.alias().orElse(null)));
    }

    public CompletionStage<Void> updateAliasByIdAsync(String id, String alias) {
        return keyRepository.updateAliasById(id, alias);
    }

    public CompletionStage<Void> deleteByIdAsync(String id) {
        return keyRepository.deleteById(id);
    }

    private record EncapsulatedKeyRecord(String id, String aliasValue) implements EncapsulatedKey {
        @Override
        public Optional<String> alias() {
            return Optional.ofNullable(aliasValue);
        }
    }
}
