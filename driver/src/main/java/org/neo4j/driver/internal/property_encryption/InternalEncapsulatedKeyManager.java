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
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;

final class InternalEncapsulatedKeyManager extends AbstractEncapsulatedKeyManager implements EncapsulatedKeyManager {
    InternalEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository) {
        super(keyEncapsulationService, keyRepository);
    }

    @Override
    public EncapsulatedKey create(String alias, KeyEncapsulationOptions encapsulationOptions) {
        return createAsync(alias, encapsulationOptions).toCompletableFuture().join();
    }

    @Override
    public Optional<EncapsulatedKey> findByAlias(String alias) {
        return Optional.ofNullable(findByAliasAsync(alias).toCompletableFuture().join());
    }

    @Override
    public void updateAliasById(String id, String alias) {
        updateAliasByIdAsync(id, alias).toCompletableFuture().join();
    }

    @Override
    public void deleteById(String id) {
        deleteByIdAsync(id).toCompletableFuture().join();
    }
}
