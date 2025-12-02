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
package org.neo4j.driver.property_encryption.reactivestreams;

import java.util.Optional;
import org.neo4j.driver.property_encryption.BaseEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.util.Preview;
import org.reactivestreams.Publisher;

/**
 * A reactive manager for encapsulated keys.
 *
 * @see org.neo4j.driver.property_encryption.EncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.async.AsyncEncapsulatedKeyManager
 * @see org.neo4j.driver.property_encryption.reactive.ReactiveEncapsulatedKeyManager
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface ReactiveEncapsulatedKeyManager extends BaseEncapsulatedKeyManager {
    /**
     * Creates a new encapsulated key without key alias.
     * @return the encapsulated key
     */
    default Publisher<EncapsulatedKey> create() {
        return create(null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias.
     * @param alias the key alias, may be {@literal null}
     * @return the encapsulated key
     */
    default Publisher<EncapsulatedKey> create(String alias) {
        return create(alias, null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias and {@link KeyEncapsulationOptions}.
     * @param alias the key alias, may be {@literal null}
     * @param encapsulationOptions the key encapsulation options, may be {@literal null}
     * @return the encapsulated key
     */
    Publisher<EncapsulatedKey> create(String alias, KeyEncapsulationOptions encapsulationOptions);

    /**
     * Finds encapsulated key by its alias.
     * @param alias the key alias, must not be {@literal null}
     * @return the encapsulated key or {@link Optional#empty()} otherwise
     */
    Publisher<EncapsulatedKey> findByAlias(String alias);

    /**
     * Updates encapsulated key alias by id.
     * @param id the key id, must not be {@literal null}
     * @param alias the new key alias, may be {@literal null}
     * @return {@link Void}
     */
    Publisher<Void> updateAliasById(String id, String alias);

    /**
     * Deletes encapsulated key alias by id.
     * @param id the key id, must not be {@literal null}
     * @return {@link Void}
     */
    default Publisher<Void> deleteAliasById(String id) {
        return updateAliasById(id, null);
    }

    /**
     * Deletes encapsulated key by id.
     * @param id the key id, must not be {@literal null}
     * @return {@link Void}
     */
    Publisher<Void> deleteById(String id);
}
