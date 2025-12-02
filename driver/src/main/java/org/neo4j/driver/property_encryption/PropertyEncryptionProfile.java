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
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.InternalEncapsulatedKey;
import org.neo4j.driver.internal.property_encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.util.Preview;

/**
 * A profile for Neo4j Property Encryption.
 * <p>
 * Each profile instance represents a specific encryption configuration that MUST have a unique name, which is also
 * used for cross-driver interoperability.
 * <p>
 * While there may be several profile types in the future, only {@link Envelope} is supported for now.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface PropertyEncryptionProfile permits PropertyEncryptionProfile.Envelope {
    // TODO decide if this should be moved
    /**
     * Creates a new instance of encryption profile that enables Envelope Encryption for Neo4j Property Encryption.
     * @param name the unique name of the profile instance
     * @param defaultKeyReference the default key reference
     * @param encapsulationService the {@link KeyEncapsulationService} implementation
     * @param keyRepository the {@link Envelope.EncapsulatedKeyRepository} implementation
     * @return the new instance of encryption profile
     */
    static Envelope envelope(
            String name,
            KeyReference defaultKeyReference,
            KeyEncapsulationService encapsulationService,
            Envelope.EncapsulatedKeyRepository keyRepository) {
        return new EnvelopePropertyEncryptionProfile(name, defaultKeyReference, encapsulationService, keyRepository);
    }

    /**
     * Returns the unique profile name.
     * @return the profile name
     */
    String name();

    /**
     * An encryption profile that enables Envelope Encryption for Neo4j Property Encryption.
     */
    sealed interface Envelope extends PropertyEncryptionProfile permits EnvelopePropertyEncryptionProfile {
        /**
         * Returns the {@link KeyEncapsulationService} used by this profile.
         * @return the encapsulation service
         */
        KeyEncapsulationService encapsulationService();

        /**
         * Returns the default {@link KeyReference} used by this profile.
         * @return the default key reference
         */
        KeyReference defaultKeyReference();

        /**
         * Returns the {@link EncapsulatedKeyRepository} used by this profile.
         * @return the key repository
         */
        EncapsulatedKeyRepository keyRepository();

        /**
         * A repository for {@link EncapsulatedKey} data.
         */
        interface EncapsulatedKeyRepository {
            /**
             * Finds and returns an {@link EncapsulatedKey} by its id.
             * @param id the key id
             * @return the key
             */
            CompletionStage<EncapsulatedKey> findById(String id);

            /**
             * Finds and returns an {@link EncapsulatedKey} by its alias.
             * @param alias the key alias
             * @return the key
             */
            CompletionStage<EncapsulatedKey> findByAlias(String alias);

            // TODO decide if metadata is a good abstraction
            /**
             * Saves the encapsulation as a key and assigns it a unique id.
             * @param alias the key alias, may be {@literal null}
             * @param encapsulation the key encapsulation, must not be {@literal null}
             * @param metadata the key metadata, must not be {@literal null}
             * @return the key
             */
            CompletionStage<EncapsulatedKey> save(String alias, byte[] encapsulation, Map<String, String> metadata);

            /**
             * Updates key alias.
             * @param id the key id, must not be {@literal null}
             * @param alias the key alias, may be {@literal null}
             * @return {@link Void}
             */
            CompletionStage<Void> updateAliasById(String id, String alias);

            /**
             * Deletes key by id.
             * @param id the key id, must not be {@literal null}
             * @return {@link Void}
             */
            CompletionStage<Void> deleteById(String id);

            /**
             * A persisted encapsulated key.
             */
            sealed interface EncapsulatedKey permits InternalEncapsulatedKey {
                // TODO decide if this should be moved
                // TODO decide if metadata is a good abstraction
                /**
                 * Returns a new instance of {@link EncapsulatedKey}.
                 * @param id the key id, must not be {@literal null}
                 * @param alias the key alias, may be {@literal null}
                 * @param encapsulation the key encapsulation, must not be {@literal null}
                 * @param metadata the key metadata, must not be {@literal null}
                 * @return the new instance of encapsulated key
                 */
                static EncapsulatedKey of(String id, String alias, byte[] encapsulation, Map<String, String> metadata) {
                    return new InternalEncapsulatedKey(id, alias, encapsulation, metadata);
                }

                /**
                 * Returns the key id.
                 * @return the key id
                 */
                String id();

                /**
                 * Returns the key alias.
                 * @return the key alias
                 */
                Optional<String> alias();

                /**
                 * Returns the key encapsulation.
                 * @return the key encapsulation
                 */
                byte[] encapsulation();

                // TODO decide if metadata is a good abstraction
                /**
                 * Returns key metadata.
                 * @return the key metadata
                 */
                Map<String, String> metadata();
            }
        }
    }

    /**
     * A key reference.
     * @param reference the key reference name
     * @param type the key reference type
     */
    record KeyReference(String reference, Type type) {
        /**
         * A key reference type.
         */
        public enum Type {
            /**
             * A reference by id.
             */
            ID,
            /**
             * A reference by alias.
             */
            ALIAS
        }
    }
}
