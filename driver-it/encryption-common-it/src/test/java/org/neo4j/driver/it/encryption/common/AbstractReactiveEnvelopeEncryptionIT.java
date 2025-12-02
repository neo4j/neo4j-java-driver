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
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;
import reactor.adapter.JdkFlowAdapter;

public abstract class AbstractReactiveEnvelopeEncryptionIT
        extends AbstractBaseEnvelopeEncryptionIT<ReactivePropertyEncryption> {

    @Override
    protected Class<ReactivePropertyEncryption> encryptionClass() {
        return ReactivePropertyEncryption.class;
    }

    @Override
    protected EncapsulatedKey createKey(String keyAlias) {
        return JdkFlowAdapter.flowPublisherToFlux(encryption.keyManager().create(keyAlias))
                .blockFirst();
    }

    @Override
    protected void setAliasById(String id, String alias) {
        JdkFlowAdapter.flowPublisherToFlux(encryption.keyManager().setAliasById(id, alias))
                .blockFirst();
    }

    @Override
    protected void deleteAliasById(String id) {
        JdkFlowAdapter.flowPublisherToFlux(encryption.keyManager().deleteAliasById(id))
                .blockFirst();
    }

    @Override
    protected void deleteKey() {
        JdkFlowAdapter.flowPublisherToFlux(encryption.keyManager().findByAlias(keyAlias))
                .flatMap(key -> JdkFlowAdapter.flowPublisherToFlux(
                        encryption.keyManager().deleteById(key.id())))
                .blockFirst();
    }

    @Override
    protected byte[] encrypt(PropertyEncryptionRequest request) {
        return JdkFlowAdapter.flowPublisherToFlux(encryption.encryptToBytes(request))
                .blockFirst();
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return JdkFlowAdapter.flowPublisherToFlux(encryption.decrypt(request)).blockFirst();
    }

    @Override
    protected Optional<EncapsulatedKey> findByAlias(String alias) {
        return Optional.ofNullable(
                JdkFlowAdapter.flowPublisherToFlux(encryption.keyManager().findByAlias(alias))
                        .blockFirst());
    }
}
