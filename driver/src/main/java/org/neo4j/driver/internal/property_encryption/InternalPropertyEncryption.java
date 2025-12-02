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
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryption;

public final class InternalPropertyEncryption extends AbstractPropertyEncryption<EncapsulatedKeyManager>
        implements PropertyEncryption {

    public InternalPropertyEncryption(
            Map<String, EncryptionHandler> nameToHandler, @SuppressWarnings("deprecation") Logging logging) {
        super(nameToHandler, logging);
    }

    @Override
    public byte[] encryptToBytes(PropertyEncryptRequest encryptRequest) {
        return encryptToBytesAsync(encryptRequest).toCompletableFuture().join();
    }

    @Override
    public Value decrypt(PropertyDecryptRequest decryptRequest) {
        return decryptAsync(decryptRequest).toCompletableFuture().join();
    }

    @Override
    public EncapsulatedKeyManager keyManager(String profileName) {
        var handler = getHandler(profileName);
        var keyRepository = handler.keyRepository();
        if (keyRepository == null) {
            throw new IllegalStateException("Key Manager is not supported in this profile");
        }
        var keyEncapsulationService = handler.keyEncapsulationService();
        return new InternalEncapsulatedKeyManager(keyEncapsulationService, keyRepository);
    }
}
