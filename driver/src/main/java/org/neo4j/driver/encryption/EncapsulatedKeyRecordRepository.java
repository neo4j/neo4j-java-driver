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
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.util.Preview;

/**
 * A repository for {@link EncapsulatedKeyRecord} data.
 * <p>
 * Implementations MUST NOT block the calling thread. Repository operations that require I/O or other potentially
 * blocking work must perform that work asynchronously and return a {@link CompletionStage} that completes when the
 * operation has finished.
 * <p>
 * Implementations MUST enforce alias uniqueness and generate globally unique and immutable key identifiers.
 *
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface EncapsulatedKeyRecordRepository {
    /**
     * Finds and returns an {@link EncapsulatedKeyRecord} by its id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with the key, or completes with {@literal null} if no key with the given id exists
     */
    CompletionStage<EncapsulatedKeyRecord> findById(String id);

    /**
     * Finds and returns an {@link EncapsulatedKeyRecord} by its alias.
     *
     * @param alias the key alias, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with the key, or completes with {@literal null} if no key with the given alias
     * exists
     */
    CompletionStage<EncapsulatedKeyRecord> findByAlias(String alias);

    /**
     * Creates the encapsulation as a key and assigns it a globally unique id.
     * <p>
     * The generated id MUST be globally unique and immutable. Implementations SHOULD use a mechanism designed to
     * generate globally unique identifiers, such as UUIDs.
     *
     * @param alias         the key alias, may be {@literal null}
     * @param encapsulation the key encapsulation, must not be {@literal null}
     * @param metadata      the key metadata, must not be {@literal null}
     * @return a {@link CompletionStage} that completes with the created key
     */
    CompletionStage<EncapsulatedKeyRecord> create(String alias, byte[] encapsulation, Map<String, String> metadata);

    /**
     * Sets the alias of an encapsulated key by id. The alias must not be used by another key. To assign an alias
     * currently used by another key, it must first be deleted from that key.
     *
     * @param id    the key id, must not be {@literal null}
     * @param alias the key alias, may be {@literal null} to remove the alias
     * @return a {@link CompletionStage} that completes when the alias has been updated
     */
    CompletionStage<Void> setAliasById(String id, String alias);

    /**
     * Deletes a key by id.
     *
     * @param id the key id, must not be {@literal null}
     * @return a {@link CompletionStage} that completes when the key has been deleted
     */
    CompletionStage<Void> deleteById(String id);
}
