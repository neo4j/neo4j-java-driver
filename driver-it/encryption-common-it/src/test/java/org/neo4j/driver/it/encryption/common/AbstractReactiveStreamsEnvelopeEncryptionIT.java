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
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption;
import reactor.core.publisher.Mono;

public abstract class AbstractReactiveStreamsEnvelopeEncryptionIT
        extends AbstractBaseEnvelopeEncryptionIT<ReactivePropertyEncryption> {

    @Override
    protected Class<ReactivePropertyEncryption> encryptionClass() {
        return ReactivePropertyEncryption.class;
    }

    @Override
    protected EncapsulatedKey createKey(String keyAlias) {
        return Mono.fromDirect(encryption.keyManager().create(keyAlias)).block();
    }

    @Override
    protected void setAliasById(String id, String alias) {
        Mono.from(encryption.keyManager().setAliasById(id, alias)).block();
    }

    @Override
    protected void deleteAliasById(String id) {
        Mono.from(encryption.keyManager().deleteAliasById(id)).block();
    }

    @Override
    protected void deleteKey() {
        Mono.fromDirect(encryption.keyManager().findByAlias(keyAlias))
                .flatMap(key -> Mono.fromDirect(encryption.keyManager().deleteById(key.id())))
                .block();
    }

    @Override
    protected byte[] encrypt(PropertyEncryptionRequest request) {
        return Mono.fromDirect(encryption.encryptToBytes(request)).block();
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return Mono.fromDirect(encryption.decrypt(request)).block();
    }

    @Override
    protected Optional<EncapsulatedKey> findByAlias(String alias) {
        return Optional.ofNullable(
                Mono.from(encryption.keyManager().findByAlias(alias)).block());
    }
}
