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
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryption;

public abstract class AbstractEnvelopeEncryptionIT
        extends AbstractBaseEnvelopeEncryptionIT<PropertyEncryption, EncapsulatedKeyManager> {

    @Override
    protected Class<PropertyEncryption> encryptionClass() {
        return PropertyEncryption.class;
    }

    @Override
    protected BaseEncapsulatedKeyManager.EncapsulatedKey createKey(String keyAlias) {
        return keyManager.create(keyAlias);
    }

    @Override
    protected void updateAliasById(String id, String alias) {
        keyManager.updateAliasById(id, alias);
    }

    @Override
    protected void deleteAliasById(String id) {
        keyManager.deleteAliasById(id);
    }

    @Override
    protected void deleteKey() {
        keyManager.findByAlias(keyAlias).ifPresent(key -> keyManager.deleteById(key.id()));
    }

    @Override
    protected Optional<BaseEncapsulatedKeyManager.EncapsulatedKey> findByAlias(String alias) {
        return keyManager.findByAlias(alias);
    }

    @Override
    protected byte[] encrypt(PropertyEncryptRequest request) {
        return encryption.encryptToBytes(request);
    }

    @Override
    protected Value decrypt(PropertyDecryptRequest request) {
        return encryption.decrypt(request);
    }
}
