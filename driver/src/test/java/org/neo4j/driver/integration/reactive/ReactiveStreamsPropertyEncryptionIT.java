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
package org.neo4j.driver.integration.reactive;

import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.encryption.reactivestreams.ReactivePropertyEncryption;
import org.neo4j.driver.integration.AbstractPropertyEncryptionIT;
import reactor.core.publisher.Mono;

class ReactiveStreamsPropertyEncryptionIT extends AbstractPropertyEncryptionIT<ReactivePropertyEncryption> {

    @Override
    protected Class<ReactivePropertyEncryption> propertyEncryptionType() {
        return ReactivePropertyEncryption.class;
    }

    @Override
    protected byte[] encryptToBytes(PropertyEncryptionRequest request) {
        return Mono.fromDirect(propertyEncryption.encryptToBytes(request)).block();
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return Mono.fromDirect(propertyEncryption.decrypt(request)).block();
    }

    @Override
    protected void createKey() {
        Mono.fromDirect(propertyEncryption.keyManager().create()).block();
    }
}
