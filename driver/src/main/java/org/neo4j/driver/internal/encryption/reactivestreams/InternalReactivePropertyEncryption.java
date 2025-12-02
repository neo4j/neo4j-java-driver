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
package org.neo4j.driver.internal.encryption.reactivestreams;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreams;

import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.encryption.reactivestreams.ReactiveEncapsulatedKeyManager;
import org.neo4j.driver.encryption.reactivestreams.ReactivePropertyEncryption;
import org.neo4j.driver.internal.encryption.AEADEncryption;
import org.neo4j.driver.internal.encryption.AbstractPropertyEncryption;
import org.neo4j.driver.internal.encryption.PropertyEncryptionHandler;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public final class InternalReactivePropertyEncryption extends AbstractPropertyEncryption<ReactiveEncapsulatedKeyManager>
        implements ReactivePropertyEncryption {
    private final DriverObservationProvider observationProvider;

    public InternalReactivePropertyEncryption(
            Map<String, PropertyEncryptionHandler> nameToHandler,
            AEADEncryption aeadEncryption,
            DriverObservationProvider observationProvider) {
        super(nameToHandler, aeadEncryption);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public Publisher<byte[]> encryptToBytes(PropertyEncryptionRequest encryptionRequest) {
        var encryptObservation = observationProvider.encryptToBytes(ReactivePropertyEncryption.class);
        return observeStreams(
                encryptObservation, Mono.fromCompletionStage(() -> encryptToBytesAsync(encryptionRequest)));
    }

    @Override
    public Publisher<Value> decrypt(PropertyDecryptionRequest decryptionRequest) {
        var decryptObservation = observationProvider.decrypt(ReactivePropertyEncryption.class);
        return observeStreams(decryptObservation, Mono.fromCompletionStage(() -> decryptAsync(decryptionRequest)));
    }

    @Override
    public ReactiveEncapsulatedKeyManager keyManager(String profileName) {
        var handler = getHandler(profileName);
        var keyRepository = handler.keyRepository();
        if (keyRepository == null) {
            throw new IllegalStateException("Key Manager is not supported in this profile");
        }
        var keyEncapsulationService = handler.keyEncapsulationService();
        return new InternalReactiveEncapsulatedKeyManager(
                keyEncapsulationService, keyRepository, handler.keyCache(), observationProvider);
    }
}
