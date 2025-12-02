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
package org.neo4j.driver.it.encryption.common;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.property_encryption.async.AsyncEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;

public abstract class AbstractAsyncEnvelopeEncryptionIT
        extends AbstractBaseEnvelopeEncryptionIT<AsyncPropertyEncryption, AsyncEncapsulatedKeyManager> {
    @Override
    protected Class<AsyncPropertyEncryption> encryptionClass() {
        return AsyncPropertyEncryption.class;
    }

    @Override
    protected BaseEncapsulatedKeyManager.EncapsulatedKey createKey(String keyAlias) {
        return keyManager.createAsync(keyAlias).toCompletableFuture().join();
    }

    @Override
    protected void updateAliasById(String id, String alias) {
        keyManager.updateAliasByIdAsync(id, alias).toCompletableFuture().join();
    }

    @Override
    protected void deleteAliasById(String id) {
        keyManager.deleteAliasByIdAsync(id).toCompletableFuture().join();
    }

    @Override
    protected void deleteKey() {
        keyManager
                .findByAliasAsync(keyAlias)
                .thenCompose(key ->
                        key == null ? CompletableFuture.completedStage(null) : keyManager.deleteByIdAsync(key.id()));
    }

    @Override
    protected byte[] encrypt(PropertyEncryptRequest request) {
        return encryption.encryptToBytesAsync(request).toCompletableFuture().join();
    }

    @Override
    protected Value decrypt(PropertyDecryptRequest request) {
        return encryption.decryptAsync(request).toCompletableFuture().join();
    }

    @Override
    protected Optional<BaseEncapsulatedKeyManager.EncapsulatedKey> findByAlias(String alias) {
        return Optional.ofNullable(
                keyManager.findByAliasAsync(alias).toCompletableFuture().join());
    }
}
