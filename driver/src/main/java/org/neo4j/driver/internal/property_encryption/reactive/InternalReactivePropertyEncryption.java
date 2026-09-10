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
package org.neo4j.driver.internal.property_encryption.reactive;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreams;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.property_encryption.AEADEncryption;
import org.neo4j.driver.internal.property_encryption.AbstractPropertyEncryption;
import org.neo4j.driver.internal.property_encryption.PropertyEncryptionHandler;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.neo4j.driver.property_encryption.reactive.ReactiveEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;
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
    public Publisher<byte[]> encryptToBytes(PropertyEncryptionRequest encryptRequest) {
        var encryptObservation = observationProvider.encryptToBytes(ReactivePropertyEncryption.class);
        return publisherToFlowPublisher(observeStreams(
                encryptObservation, Mono.fromCompletionStage(() -> encryptToBytesAsync(encryptRequest))));
    }

    @Override
    public Publisher<Value> decrypt(PropertyDecryptionRequest decryptRequest) {
        var decryptObservation = observationProvider.decrypt(ReactivePropertyEncryption.class);
        return publisherToFlowPublisher(
                observeStreams(decryptObservation, Mono.fromCompletionStage(() -> decryptAsync(decryptRequest))));
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
