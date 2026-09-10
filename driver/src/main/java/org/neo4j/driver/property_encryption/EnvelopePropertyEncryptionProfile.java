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

import java.security.Provider;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Optional;
import org.neo4j.driver.internal.property_encryption.InternalEnvelopePropertyEncryptionProfile;
import org.neo4j.driver.util.Preview;

/**
 * An encryption profile that enables Envelope Encryption for Neo4j Property Encryption.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface EnvelopePropertyEncryptionProfile extends PropertyEncryptionProfile
        permits InternalEnvelopePropertyEncryptionProfile {
    /**
     * Returns a new builder for {@link EnvelopePropertyEncryptionProfile}.
     * @param name the unique name of the profile instance, must not be {@literal null} or empty
     * @param keyEncapsulationService the {@link KeyEncapsulationService} implementation, must not be {@literal null}
     * @param keyRepository the {@link EncapsulatedKeyRecordRepository} implementation, must not be {@literal null}
     * @return the new builder
     */
    static Builder builder(
            String name,
            KeyEncapsulationService keyEncapsulationService,
            EncapsulatedKeyRecordRepository keyRepository) {
        return new InternalEnvelopePropertyEncryptionProfile.Builder(name, keyEncapsulationService, keyRepository);
    }

    /**
     * A builder for {@link EnvelopePropertyEncryptionProfile}.
     */
    interface Builder {
        /**
         * Configures the {@link CryptoContext} to be used for cryptographic operations.
         *
         * @param provider the {@link Provider}, must not be {@literal null}
         * @param ivSecureRandom the {@link SecureRandom} for IV generation, must not be {@literal null} and
         * {@link SecureRandom#getProvider()} must resolve to the provider parameter
         * @return this builder
         */
        Builder withCryptoContext(Provider provider, SecureRandom ivSecureRandom);

        /**
         * Configures the key cache.
         * <p>
         * The key cache stores mappings from key ids to decapsulated keys. This is especially useful when
         * {@link EncapsulatedKeyRecordRepository} and/or {@link KeyEncapsulationService} require network exchanges.
         * <p>
         * The cache is enabled by default with a maximum size of {@literal 100} entries and an entry TTL of {@literal 15}
         * minutes. When adding a new entry, expired entries are purged first. If the cache is still at its maximum size,
         * the least recently used entry is evicted.
         * <p>
         * Key ids are expected to be globally unique, so the TTL can be configured to be longer if avoiding repeated key
         * decapsulation is preferred. A longer TTL also means that decapsulated keys remain in memory for longer and are
         * not refreshed or removed from the cache as frequently.
         *
         * @param maxSize the maximum cache size, must be greater than or equal to {@literal 1}
         * @param ttl the entry TTL, must not be {@literal null}
         * @return this builder
         */
        Builder withKeyCache(int maxSize, Duration ttl);

        /**
         * Disables the key cache.
         * <p>
         * Disabling the key cache also disables the key alias index.
         *
         * @return this builder
         */
        Builder withoutKeyCache();

        /**
         * Configures the key alias index.
         * <p>
         * The key alias index stores mappings from key aliases to key ids. This is especially useful when
         * {@link EncapsulatedKeyRecordRepository} and/or {@link KeyEncapsulationService} require network exchanges.
         * The key alias index can only be enabled when the key cache is enabled.
         * <p>
         * The index is enabled by default with a maximum size of {@literal 100} entries and an entry TTL of {@literal 15}
         * seconds.
         * <p>
         * A shorter TTL allows an alias to be removed from one key and assigned to another key while limiting the period
         * during which a driver may use a cached mapping to the previous key, allowing for predictable alias reassignment.
         *
         * @param maxSize the maximum cache size, must be greater than or equal to {@literal 1}
         * @param ttl the entry TTL, must not be {@literal null}
         * @return this builder
         */
        Builder withKeyAliasIndex(int maxSize, Duration ttl);

        /**
         * Disables the key alias index.
         *
         * @return this builder
         */
        Builder withoutKeyAliasIndex();

        /**
         * Returns a new instance of {@link EnvelopePropertyEncryptionProfile}.
         * @return the new instance of profile
         */
        EnvelopePropertyEncryptionProfile build();
    }

    /**
     * Returns the {@link KeyEncapsulationService} used by this profile.
     * @return the encapsulation service
     */
    KeyEncapsulationService keyEncapsulationService();

    /**
     * Returns the {@link EncapsulatedKeyRecordRepository} used by this profile.
     * @return the key repository
     */
    EncapsulatedKeyRecordRepository keyRepository();

    /**
     * Returns {@link CryptoContext} if set.
     * @return the crypto context
     */
    Optional<CryptoContext> cryptoContext();

    /**
     * Returns the key cache {@link CacheConfig} if enabled.
     * <p>
     * The cache is enabled by default with a maximum size of {@literal 100} entries and a TTL of {@literal 15} minutes.
     *
     * @return the cache config
     */
    Optional<CacheConfig> keyCacheConfig();

    /**
     * Returns the key alias index {@link CacheConfig} if enabled.
     * <p>
     * The cache is enabled by default with a maximum size of {@literal 100} entries and a TTL of {@literal 15} seconds.
     *
     * @return the cache config
     */
    Optional<CacheConfig> keyAliasIndexConfig();
}
