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
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.value.BoltValueFactory;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;

public abstract class AbstractPropertyEncryption<T extends BaseEncapsulatedKeyManager>
        implements BasePropertyEncryption<T> {
    protected final Map<String, EncryptionHandler> nameToHandler;
    private final AEADEncryption aeadEncryption;

    protected AbstractPropertyEncryption(
            Map<String, EncryptionHandler> nameToHandler, @SuppressWarnings("deprecation") Logging logging) {
        this.nameToHandler = Objects.requireNonNull(nameToHandler);
        this.aeadEncryption = new AEADEncryption(BoltValueFactory.getInstance(), logging);
    }

    @Override
    public PropertyEncryptRequest.ValueStep encryptRequest() {
        return new InternalPropertyEncryptRequest();
    }

    @Override
    public PropertyDecryptRequest.ValueStep decryptRequest() {
        return new InternalPropertyDecryptRequest();
    }

    protected EncryptionHandler getHandler(String profileName) {
        if (profileName != null) {
            var handler = nameToHandler.get(profileName);
            if (handler == null) {
                throw new ClientException("No handler was found for profile " + profileName);
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

    public CompletionStage<byte[]> encryptToBytesAsync(PropertyEncryptRequest encryptRequest) {
        var request = (InternalPropertyEncryptRequest) encryptRequest;
        return getHandler(request.profileName).encrypt(request);
    }

    public CompletionStage<Value> decryptAsync(PropertyDecryptRequest decryptRequest) {
        var request = (InternalPropertyDecryptRequest) decryptRequest;
        var encryptedProperty = aeadEncryption.unpackAEAD(request.value);
        return getHandler(encryptedProperty.profileName()).decrypt(request, encryptedProperty);
    }
}
