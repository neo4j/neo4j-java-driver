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

import java.util.Map;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.property_encryption.InternalEncapsulationResult;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager.EncapsulatedKey;
import org.neo4j.driver.util.Preview;

/**
 * A service responsible for encapsulating and decapsulating keys.
 * @see KeyEncapsulationOptions
 * @see EncapsulatedKey
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface KeyEncapsulationService {
    /**
     * Creates a new key, encapsulates it and returns the result.
     * @param options the encapsulation options
     * @return the encapsulation result
     */
    CompletionStage<EncapsulationResult> encapsulate(KeyEncapsulationOptions options);

    /**
     * Decapsulates encapsulated bytes.
     * @param encapsulation the encapsulated bytes
     * @param options the encapsulation options represented as a map
     * @return the key
     */
    CompletionStage<SecretKey> decapsulate(byte[] encapsulation, Map<String, String> options);

    /**
     * A key encapsulation result.
     */
    interface EncapsulationResult {
        /**
         * Creates a new encapsulation result.
         * @param encapsulation the encapsulation bytes
         * @param options the encapsulation options
         * @param key the new key
         * @return the new encapsulation result
         */
        // TODO decide if this should be moved
        static EncapsulationResult of(byte[] encapsulation, KeyEncapsulationOptions options, SecretKey key) {
            return new InternalEncapsulationResult(encapsulation, options, key);
        }

        /**
         * Returns the encapsulation bytes.
         * @return the encapsulation bytes
         */
        byte[] encapsulation();

        /**
         * Returns the encapsulation options.
         * @return the encapsulation options
         */
        KeyEncapsulationOptions options();

        /**
         * Returns the key.
         * @return the key
         */
        SecretKey key();
    }
}
