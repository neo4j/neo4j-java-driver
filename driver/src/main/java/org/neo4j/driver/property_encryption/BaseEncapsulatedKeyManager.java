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

import java.util.Optional;
import org.neo4j.driver.util.Preview;

/**
 * A base type for encapsulated key managers.
 * @see EncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.async.AsyncEncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.reactive.ReactiveEncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactiveEncapsulatedKeyManager
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface BaseEncapsulatedKeyManager {
    /**
     * An encapsulated key.
     */
    interface EncapsulatedKey {
        /**
         * Returns key id.
         * @return the key id
         */
        String id();

        /**
         * Returns key alias if assigned.
         * @return the key alias or {@link Optional#empty()} otherwise
         */
        Optional<String> alias();
    }
}
