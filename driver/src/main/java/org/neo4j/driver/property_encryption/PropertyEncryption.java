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
package org.neo4j.driver.property_encryption;

import org.neo4j.driver.Value;
import org.neo4j.driver.util.Preview;

/**
 * A Neo4j Property encryption.
 * @see org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface PropertyEncryption extends BasePropertyEncryption {
    /**
     * Handles the provided {@link PropertyEncryptionRequest}.
     * @param encryptRequest the request, must not be {@literal null}
     * @return the encrypted bytes
     */
    byte[] encryptToBytes(PropertyEncryptionRequest encryptRequest);

    /**
     * Handles the provided {@link PropertyDecryptionRequest}.
     * @param decryptRequest the request, must not be {@literal null}
     * @return the decrypted value
     */
    Value decrypt(PropertyDecryptionRequest decryptRequest);

    /**
     * Returns key manager.
     * @return key manager
     */
    default EncapsulatedKeyManager keyManager() {
        return keyManager(null);
    }

    /**
     * Returns key manager for a given profile name.
     * @param profileName the profile name
     * @return key manager, may be {@literal null} when only a single profile is available
     */
    EncapsulatedKeyManager keyManager(String profileName);
}
