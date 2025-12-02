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

import org.neo4j.driver.util.Preview;

/**
 * A base interface for Neo4j Property encryption.
 * @param <T> encapsulated key manager type
 * @see PropertyEncryption
 * @see org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface BasePropertyEncryption<T extends BaseEncapsulatedKeyManager> {
    /**
     * Returns a new instance of a build stage for {@link PropertyEncryptRequest}.
     * @return a new instance of a build stage
     */
    PropertyEncryptRequest.ValueStep encryptRequest();

    /**
     * Returns a new instance of a build stage for {@link PropertyDecryptRequest}.
     * @return a new instance of a build stage
     */
    PropertyDecryptRequest.ValueStep decryptRequest();

    /**
     * Returns key manager.
     * @return key manager
     */
    default T keyManager() {
        return keyManager(null);
    }

    /**
     * Returns key manager for a given profile name.
     * @param profileName the profile name
     * @return key manager, may be {@literal null} when only a single profile is available
     */
    T keyManager(String profileName);
}
