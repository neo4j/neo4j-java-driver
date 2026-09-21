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

import java.util.Map;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.encryption.InternalKeyEncapsulationResult;
import org.neo4j.driver.util.Preview;

/**
 * The result of a key encapsulation operation.
 * <p>
 * Contains the encapsulated key, the metadata required to decapsulate it, and the key itself.
 *
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface KeyEncapsulationResult permits InternalKeyEncapsulationResult {
    /**
     * Returns a new instance of {@link KeyEncapsulationResult}.
     *
     * @param encapsulation the encapsulation bytes, must not be {@literal null}
     * @param metadata      the encapsulation metadata, must not be {@literal null}
     * @param key           the new key, must not be {@literal null}
     * @return the new encapsulation result
     */
    static KeyEncapsulationResult of(byte[] encapsulation, Map<String, String> metadata, SecretKey key) {
        return new InternalKeyEncapsulationResult(encapsulation, metadata, key);
    }

    /**
     * Returns the encapsulated key.
     *
     * @return the encapsulation bytes
     */
    byte[] encapsulation();

    /**
     * Returns the metadata associated with the encapsulated key.
     * <p>
     * The metadata is provided to the {@link KeyEncapsulationService} when the key is decapsulated.
     *
     * @return the key metadata
     */
    Map<String, String> metadata();

    /**
     * Returns the key.
     *
     * @return the key
     */
    SecretKey key();
}
