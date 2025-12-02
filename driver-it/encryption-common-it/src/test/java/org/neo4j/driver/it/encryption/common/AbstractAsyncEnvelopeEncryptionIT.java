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
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;

public abstract class AbstractAsyncEnvelopeEncryptionIT
        extends AbstractBaseEnvelopeEncryptionIT<AsyncPropertyEncryption> {
    @Override
    protected Class<AsyncPropertyEncryption> encryptionClass() {
        return AsyncPropertyEncryption.class;
    }

    @Override
    protected EncapsulatedKey createKey(String keyAlias) {
        return encryption
                .keyManager()
                .createAsync(keyAlias)
                .toCompletableFuture()
                .join();
    }

    @Override
    protected void setAliasById(String id, String alias) {
        encryption
                .keyManager()
                .setAliasByIdAsync(id, alias)
                .toCompletableFuture()
                .join();
    }

    @Override
    protected void deleteAliasById(String id) {
        encryption.keyManager().deleteAliasByIdAsync(id).toCompletableFuture().join();
    }

    @Override
    protected void deleteKey() {
        encryption
                .keyManager()
                .findByAliasAsync(keyAlias)
                .thenCompose(key -> key == null
                        ? CompletableFuture.completedStage(null)
                        : encryption.keyManager().deleteByIdAsync(key.id()));
    }

    @Override
    protected byte[] encrypt(PropertyEncryptionRequest request) {
        return encryption.encryptToBytesAsync(request).toCompletableFuture().join();
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return encryption.decryptAsync(request).toCompletableFuture().join();
    }

    @Override
    protected Optional<EncapsulatedKey> findByAlias(String alias) {
        return Optional.ofNullable(encryption
                .keyManager()
                .findByAliasAsync(alias)
                .toCompletableFuture()
                .join());
    }
}
