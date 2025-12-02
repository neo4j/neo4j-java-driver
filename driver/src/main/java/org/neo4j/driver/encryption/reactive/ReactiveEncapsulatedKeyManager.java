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
package org.neo4j.driver.encryption.reactive;

import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.encryption.EncapsulatedKey;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationService;
import org.neo4j.driver.util.Preview;

/**
 * A reactive manager for encapsulated keys.
 * <p>
 * {@link EnvelopePropertyEncryptionProfile} requires data keys to exist before they can be used for encryption. This
 * manager provides operations for creating and managing such keys.
 * <p>
 * When creating a key, the manager uses the configured {@link KeyEncapsulationService} to generate and encapsulate a
 * new data key and registers the resulting {@link EncapsulatedKeyRecord} with the configured
 * {@link EncapsulatedKeyRecordRepository}.
 *
 * @see org.neo4j.driver.encryption.EncapsulatedKeyManager
 * @see org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyManager
 * @see org.neo4j.driver.encryption.reactivestreams.ReactiveEncapsulatedKeyManager
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface ReactiveEncapsulatedKeyManager {
    /**
     * Creates a new encapsulated key without key alias.
     *
     * @return a {@link Publisher} that publishes the encapsulated key
     */
    default Publisher<EncapsulatedKey> create() {
        return create(null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias.
     *
     * @param alias the key alias, may be {@literal null}
     * @return a {@link Publisher} that publishes the encapsulated key
     */
    default Publisher<EncapsulatedKey> create(String alias) {
        return create(alias, null);
    }

    /**
     * Creates a new encapsulated key with the provided key alias and {@link KeyEncapsulationOptions}.
     *
     * @param alias                the key alias, may be {@literal null}
     * @param encapsulationOptions the key encapsulation options, may be {@literal null}
     * @return a {@link Publisher} that publishes the encapsulated key
     */
    Publisher<EncapsulatedKey> create(String alias, KeyEncapsulationOptions encapsulationOptions);

    /**
     * Finds encapsulated key by its alias.
     *
     * @param alias the key alias, must not be {@literal null}
     * @return a {@link Publisher} that publishes the encapsulated key or completes without publishing when no key with
     * the given alias exists
     */
    Publisher<EncapsulatedKey> findByAlias(String alias);

    /**
     * Sets the alias of an encapsulated key by id. The alias must not be used by another key. To assign an alias
     * currently used by another key, it must first be deleted from that key.
     *
     * @param id    the key id, must not be {@literal null}
     * @param alias the new key alias, may be {@literal null} to remove the alias
     * @return a {@link Publisher} that completes without publishing
     */
    Publisher<Void> setAliasById(String id, String alias);

    /**
     * Deletes encapsulated key alias by id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link Publisher} that completes without publishing
     */
    default Publisher<Void> deleteAliasById(String id) {
        return setAliasById(id, null);
    }

    /**
     * Deletes encapsulated key by id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link Publisher} that completes without publishing
     */
    Publisher<Void> deleteById(String id);
}
