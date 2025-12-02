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
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;

public abstract class AbstractEnvelopeEncryptionIT extends AbstractBaseEnvelopeEncryptionIT<PropertyEncryption> {

    @Override
    protected Class<PropertyEncryption> encryptionClass() {
        return PropertyEncryption.class;
    }

    @Override
    protected EncapsulatedKey createKey(String keyAlias) {
        return encryption.keyManager().create(keyAlias);
    }

    @Override
    protected void setAliasById(String id, String alias) {
        encryption.keyManager().setAliasById(id, alias);
    }

    @Override
    protected void deleteAliasById(String id) {
        encryption.keyManager().deleteAliasById(id);
    }

    @Override
    protected void deleteKey() {
        encryption.keyManager().findByAlias(keyAlias).ifPresent(key -> encryption
                .keyManager()
                .deleteById(key.id()));
    }

    @Override
    protected Optional<EncapsulatedKey> findByAlias(String alias) {
        return encryption.keyManager().findByAlias(alias);
    }

    @Override
    protected byte[] encrypt(PropertyEncryptionRequest request) {
        return encryption.encryptToBytes(request);
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return encryption.decrypt(request);
    }
}
