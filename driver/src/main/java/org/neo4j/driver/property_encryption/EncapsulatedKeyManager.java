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
 * A manager for encapsulated keys.
 * @see org.neo4j.driver.property_encryption.async.AsyncEncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.reactive.ReactiveEncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactiveEncapsulatedKeyManager
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface EncapsulatedKeyManager {
    /**
     * Creates a new encapsulated key without key alias.
     * @return the encapsulated key
     */
    default EncapsulatedKey create() {
        return create(null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias.
     * @param alias the key alias, may be {@literal null}
     * @return the encapsulated key
     */
    default EncapsulatedKey create(String alias) {
        return create(alias, null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias and {@link KeyEncapsulationOptions}.
     * @param alias the key alias, may be {@literal null}
     * @param encapsulationOptions the key encapsulation options, may be {@literal null}
     * @return the encapsulated key
     */
    EncapsulatedKey create(String alias, KeyEncapsulationOptions encapsulationOptions);

    /**
     * Finds encapsulated key by its alias.
     * @param alias the key alias, must not be {@literal null}
     * @return the encapsulated key or {@link Optional#empty()} otherwise
     */
    Optional<EncapsulatedKey> findByAlias(String alias);

    /**
     * Sets the alias of an encapsulated key by id. The alias must not be used by another key. To assign an alias
     * currently used by another key, it must first be deleted from that key.
     * @param id the key id, must not be {@literal null}
     * @param alias the new key alias, may be {@literal null} to remove the alias
     */
    void setAliasById(String id, String alias);

    /**
     * Deletes encapsulated key alias by id.
     * @param id the key id, must not be {@literal null}
     */
    default void deleteAliasById(String id) {
        setAliasById(id, null);
    }

    /**
     * Deletes encapsulated key by id.
     * @param id the key id, must not be {@literal null}
     */
    void deleteById(String id);
}
