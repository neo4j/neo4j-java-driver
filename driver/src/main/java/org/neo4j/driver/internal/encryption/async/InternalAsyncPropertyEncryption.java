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
package org.neo4j.driver.internal.encryption.async;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyManager;
import org.neo4j.driver.encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.internal.encryption.AEADEncryption;
import org.neo4j.driver.internal.encryption.AbstractPropertyEncryption;
import org.neo4j.driver.internal.encryption.PropertyEncryptionHandler;
import org.neo4j.driver.internal.observation.DriverObservationProvider;

public final class InternalAsyncPropertyEncryption extends AbstractPropertyEncryption<AsyncEncapsulatedKeyManager>
        implements AsyncPropertyEncryption {
    private final DriverObservationProvider observationProvider;

    public InternalAsyncPropertyEncryption(
            Map<String, PropertyEncryptionHandler> nameToHandler,
            AEADEncryption aeadEncryption,
            DriverObservationProvider observationProvider) {
        super(nameToHandler, aeadEncryption);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<byte[]> encryptToBytesAsync(PropertyEncryptionRequest encryptionRequest) {
        var encryptObservation = observationProvider.encryptToBytes(AsyncPropertyEncryption.class);
        return observeAsync(encryptObservation, () -> super.encryptToBytesAsync(encryptionRequest));
    }

    @Override
    public CompletionStage<Value> decryptAsync(PropertyDecryptionRequest decryptionRequest) {
        var decryptObservation = observationProvider.decrypt(AsyncPropertyEncryption.class);
        return observeAsync(decryptObservation, () -> super.decryptAsync(decryptionRequest));
    }

    @Override
    public AsyncEncapsulatedKeyManager keyManager(String profileName) {
        var handler = getHandler(profileName);
        var keyRepository = handler.keyRepository();
        if (keyRepository == null) {
            throw new IllegalStateException("Key Manager is not supported in this profile");
        }
        var keyEncapsulationService = handler.keyEncapsulationService();
        return new InternalAsyncEncapsulatedKeyManager(
                keyEncapsulationService, keyRepository, handler.keyCache(), observationProvider);
    }
}
