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
package org.neo4j.driver.encryption;

import org.neo4j.driver.Value;
import org.neo4j.driver.util.Preview;

/**
 * Provides Neo4j Property Encryption functions.
 *
 * @see org.neo4j.driver.encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.encryption.reactivestreams.ReactivePropertyEncryption
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface PropertyEncryption extends BasePropertyEncryption {
    /**
     * Encrypts the value according to the provided {@link PropertyEncryptionRequest}.
     *
     * @param encryptionRequest the encryption request, must not be {@literal null}
     * @return the encrypted bytes
     */
    byte[] encryptToBytes(PropertyEncryptionRequest encryptionRequest);

    /**
     * Decrypts the value according to the provided {@link PropertyDecryptionRequest}.
     *
     * @param decryptionRequest the decryption request, must not be {@literal null}
     * @return the decrypted value
     */
    Value decrypt(PropertyDecryptionRequest decryptionRequest);

    /**
     * Returns the key manager.
     * <p>
     * If this instance has been configured with a single property encryption profile, the key manager for that
     * profile is returned. This method MUST NOT be used when multiple profiles are configured.
     *
     * @return the key manager
     * @throws IllegalStateException if key management is not supported by the specified profile or when no profile is
     *                               enabled
     */
    default EncapsulatedKeyManager keyManager() {
        return keyManager(null);
    }

    /**
     * Returns the key manager for the specified property encryption profile.
     *
     * @param profileName the profile name, may be {@literal null} when only a single profile is configured
     * @return the key manager
     * @throws IllegalStateException if key management is not supported by the specified profile or when no profile with
     *                               the supplied name is found
     */
    EncapsulatedKeyManager keyManager(String profileName);
}
