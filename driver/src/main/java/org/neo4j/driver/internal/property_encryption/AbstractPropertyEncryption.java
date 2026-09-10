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
package org.neo4j.driver.internal.property_encryption;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;

public abstract class AbstractPropertyEncryption<T> implements BasePropertyEncryption {
    protected final Map<String, PropertyEncryptionHandler> nameToHandler;
    private final AEADEncryption aeadEncryption;

    protected AbstractPropertyEncryption(
            Map<String, PropertyEncryptionHandler> nameToHandler, AEADEncryption aeadEncryption) {
        this.nameToHandler = Objects.requireNonNull(nameToHandler);
        this.aeadEncryption = Objects.requireNonNull(aeadEncryption);
    }

    protected PropertyEncryptionHandler getHandler(String profileName) {
        if (profileName != null) {
            var handler = nameToHandler.get(profileName);
            if (handler == null) {
                throw new ClientException("No handler was found for profile name " + profileName);
            }
            return handler;
        } else {
            if (nameToHandler.size() == 1) {
                return nameToHandler.values().iterator().next();
            } else {
                throw new ClientException("Explicit profile name is required as multiple profiles are registered");
            }
        }
    }

    public CompletionStage<byte[]> encryptToBytesAsync(PropertyEncryptionRequest encryptRequest) {
        Objects.requireNonNull(encryptRequest);
        var request = (InternalPropertyEncryptionRequest) encryptRequest;
        return getHandler(request.profileName).encrypt(request);
    }

    public CompletionStage<Value> decryptAsync(PropertyDecryptionRequest decryptRequest) {
        Objects.requireNonNull(decryptRequest);
        var request = (InternalPropertyDecryptionRequest) decryptRequest;
        AEADEncryptedProperty encryptedProperty;
        try {
            encryptedProperty = aeadEncryption.decodeEncryptedBytes(request.value);
        } catch (Exception e) {
            return CompletableFuture.failedStage(e);
        }
        return getHandler(encryptedProperty.profileName()).decrypt(request, encryptedProperty);
    }
}
