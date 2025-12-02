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
package org.neo4j.driver.internal.property_encryption.reactivestreams;

import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.property_encryption.AbstractPropertyEncryption;
import org.neo4j.driver.internal.property_encryption.EncryptionHandler;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.property_encryption.reactivestreams.ReactiveEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public final class InternalReactivePropertyEncryption extends AbstractPropertyEncryption<ReactiveEncapsulatedKeyManager>
        implements ReactivePropertyEncryption {
    public InternalReactivePropertyEncryption(
            Map<String, EncryptionHandler> nameToHandler, @SuppressWarnings("deprecation") Logging logging) {
        super(nameToHandler, logging);
    }

    @Override
    public Publisher<byte[]> encryptToBytes(PropertyEncryptRequest encryptRequest) {
        return Mono.fromCompletionStage(() -> encryptToBytesAsync(encryptRequest));
    }

    @Override
    public Publisher<Value> decrypt(PropertyDecryptRequest decryptRequest) {
        return Mono.fromCompletionStage(() -> decryptAsync(decryptRequest));
    }

    @Override
    public ReactiveEncapsulatedKeyManager keyManager() {
        var handler = getHandler(null);
        var keyRepository = handler.keyRepository();
        if (keyRepository == null) {
            throw new IllegalStateException("Key Manager is not supported in this profile");
        }
        var keyEncapsulationService = handler.keyEncapsulationService();
        return new InternalReactiveEncapsulatedKeyManager(keyEncapsulationService, keyRepository);
    }

    @Override
    public ReactiveEncapsulatedKeyManager keyManager(String profileName) {
        Objects.requireNonNull(profileName);
        var handler = getHandler(profileName);
        var keyRepository = handler.keyRepository();
        if (keyRepository == null) {
            throw new IllegalStateException("Key Manager is not supported in this profile");
        }
        var keyEncapsulationService = handler.keyEncapsulationService();
        return new InternalReactiveEncapsulatedKeyManager(keyEncapsulationService, keyRepository);
    }
}
