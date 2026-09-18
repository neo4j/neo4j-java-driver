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
package org.neo4j.driver.encryption.async;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.encryption.BaseEncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.BaseKeyEncapsulationService;
import org.neo4j.driver.encryption.EncapsulatedKey;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.util.Preview;

/**
 * An asynchronous manager for encapsulated keys.
 * <p>
 * {@link EnvelopePropertyEncryptionProfile} requires data keys to exist before they can be used for encryption. This
 * manager provides asynchronous operations for creating and managing such keys.
 * <p>
 * When creating a key, the manager uses the configured {@link BaseKeyEncapsulationService} to generate and encapsulate
 * a new data key and registers the resulting {@link EncapsulatedKeyRecord} with the configured
 * {@link BaseEncapsulatedKeyRecordRepository}. Both synchronous and asynchronous implementations of these services
 * are supported.
 *
 * @see org.neo4j.driver.encryption.EncapsulatedKeyManager
 * @see org.neo4j.driver.encryption.reactive.ReactiveEncapsulatedKeyManager
 * @see org.neo4j.driver.encryption.reactivestreams.ReactiveEncapsulatedKeyManager
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface AsyncEncapsulatedKeyManager {
    /**
     * Creates a new encapsulated key without an alias.
     *
     * @return a {@link CompletionStage} that completes with the encapsulated key
     */
    default CompletionStage<EncapsulatedKey> createAsync() {
        return createAsync(null);
    }

    /**
     * Creates a new encapsulated key with the provided alias.
     *
     * @param alias the key alias, may be {@literal null}
     * @return a {@link CompletionStage} that completes with the encapsulated key
     */
    default CompletionStage<EncapsulatedKey> createAsync(String alias) {
        return createAsync(alias, null);
    }

    /**
     * Creates a new encapsulated key with the provided alias and {@link KeyEncapsulationOptions}.
     *
     * @param alias                the key alias, may be {@literal null}
     * @param encapsulationOptions the key encapsulation options, may be {@literal null}
     * @return a {@link CompletionStage} that completes with the encapsulated key
     */
    CompletionStage<EncapsulatedKey> createAsync(String alias, KeyEncapsulationOptions encapsulationOptions);

    /**
     * Finds an encapsulated key by its alias.
     *
     * @param alias the key alias, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with the encapsulated key or {@literal null} when no key with
     * the given alias exists
     */
    CompletionStage<EncapsulatedKey> findByAliasAsync(String alias);

    /**
     * Sets the alias of an encapsulated key by id. The alias must not be used by another key. To assign an alias
     * currently used by another key, it must first be deleted from that key.
     *
     * @param id    the key id, must not be {@literal null}
     * @param alias the new key alias, may be {@literal null} to remove the alias
     * @return a {@link CompletionStage} that completes with {@literal null}
     */
    CompletionStage<Void> setAliasByIdAsync(String id, String alias);

    /**
     * Deletes the alias from encapsulated key by id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with {@literal null}
     */
    default CompletionStage<Void> deleteAliasByIdAsync(String id) {
        return setAliasByIdAsync(id, null);
    }

    /**
     * Deletes an encapsulated key by id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with {@literal null}
     */
    CompletionStage<Void> deleteByIdAsync(String id);
}
