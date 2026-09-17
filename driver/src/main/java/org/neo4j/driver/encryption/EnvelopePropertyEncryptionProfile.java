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

import java.security.Provider;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Optional;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.encryption.InternalEnvelopePropertyEncryptionProfile;
import org.neo4j.driver.util.Preview;

/**
 * An encryption profile that enables Envelope Encryption for Neo4j Property Encryption.
 * <p>
 * Envelope encryption separates encryption of data from protection of the key used to encrypt that data. Property
 * values are encrypted using a 256-bit data encryption key with AES-GCM ({@literal "AES/GCM/NoPadding"} specifically).
 * The key and its corresponding encapsulation are produced by a user-provided {@link KeyEncapsulationService}.
 * <p>
 * Each encryption operation uses a 96-bit (12-byte) initialization vector (IV). AES-GCM uses a 128-bit (16-byte)
 * authentication tag to provide integrity and authenticity of the encrypted data and any associated authenticated data
 * (AAD).
 * <p>
 * AAD is optional and is supplied explicitly when encrypting a value. The AAD is stored with the encrypted value and
 * is used during decryption unless AAD is explicitly supplied by the caller.
 * <p>
 * Both the {@link Provider} used for AES-GCM and the {@link SecureRandom} from which IVs are sourced are configurable.
 * If neither is explicitly provided, the Java runtime determines and provides them according to its configuration.
 * <p>
 * The encapsulation and associated metadata are stored in a user-provided {@link EncapsulatedKeyRecordRepository}.
 * When a property is encrypted, the driver obtains the corresponding encapsulated key from the repository and uses
 * the {@link KeyEncapsulationService} to decapsulate the data key. The key is then used for AES-GCM encryption. When a
 * property is decrypted, the driver similarly obtains the encapsulated key and uses the service to decapsulate the key
 * required for decryption.
 * <p>
 * The key encapsulation mechanism is implementation-specific. It may use key wrapping, symmetric or asymmetric
 * cryptography, a key management service (KMS), or a post-quantum key encapsulation mechanism such as ML-KEM. For
 * example, the driver provides a local {@link KeyEncapsulationServices#local(SecretKey)} implementation that
 * encapsulates and decapsulates data keys using the provided AES-256 master key. Additionally, three optional modules
 * are available that provide implementations using Google Cloud KMS, AWS KMS and Azure Key Vault.
 * <p>
 * A key can be referenced by its globally unique identifier or, if assigned, by its alias. An alias is a mutable
 * application-level reference and may be reassigned to a different key over time.
 * <p>
 * By default, the driver caches decapsulated data encryption keys to avoid repeated key resolution. Depending on the
 * {@link KeyEncapsulationService} and {@link EncapsulatedKeyRecordRepository} implementations, resolving a key may also
 * require network exchanges. The cache is keyed by the key's globally unique identifier and is subject to a
 * configurable maximum size and time-to-live (TTL).
 * <p>
 * Aliases are resolved separately through a key alias index that maps aliases to key identifiers. The alias index does
 * not contain key material and has its own configurable size and TTL. This allows alias mappings to expire
 * independently of cached keys and limits the period for which a driver may use a stale alias after it has been
 * reassigned, providing predictability during alias reassignment.
 * <p>
 * Both caches are bounded and use a least-recently-used (LRU) eviction policy when their configured maximum size is
 * reached. Entries that have exceeded their configured TTL are treated as cache misses and are not used.
 * <p>
 * The key cache may be disabled. When the key cache is disabled, the key alias index is also disabled. The key alias
 * index may also be disabled independently.
 *
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface EnvelopePropertyEncryptionProfile extends PropertyEncryptionProfile
        permits InternalEnvelopePropertyEncryptionProfile {
    /**
     * Returns a new builder for {@link EnvelopePropertyEncryptionProfile}.
     *
     * @param name                    the unique name of the profile instance, must not be {@literal null} or empty
     * @param keyEncapsulationService the {@link KeyEncapsulationService} implementation, must not be {@literal null}
     * @param keyRepository           the {@link EncapsulatedKeyRecordRepository} implementation, must not be {@literal null}
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
         * <p>
         * The supplied {@link Provider} is used to create the {@link javax.crypto.Cipher} for
         * {@literal "AES/GCM/NoPadding"}. The provider must support this cipher transformation.
         * The supplied {@link SecureRandom} is used as the source of the 12-byte initialization
         * vectors (IVs) required for encryption.
         *
         * @param provider       the {@link Provider}, must not be {@literal null}
         * @param ivSecureRandom the {@link SecureRandom} for IV generation, must not be {@literal null} and
         *                       {@link SecureRandom#getProvider()} must resolve to the provider parameter
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
         * @param maxSize the maximum cache size, must be greater than {@literal 0}
         * @param ttl     the entry TTL, must not be {@literal null}, {@link Duration#isNegative()} or
         *                {@link Duration#isZero()}
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
         * @param maxSize the maximum cache size, must be greater than {@literal 0}
         * @param ttl     the entry TTL, must not be {@literal null}, {@link Duration#isNegative()} or
         *                {@link Duration#isZero()}
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
         *
         * @return the new instance of profile
         */
        EnvelopePropertyEncryptionProfile build();
    }

    /**
     * Returns the {@link KeyEncapsulationService} used by this profile.
     *
     * @return the encapsulation service
     */
    KeyEncapsulationService keyEncapsulationService();

    /**
     * Returns the {@link EncapsulatedKeyRecordRepository} used by this profile.
     *
     * @return the key repository
     */
    EncapsulatedKeyRecordRepository keyRepository();

    /**
     * Returns {@link CryptoContext} if set.
     * <p>
     * If no context is configured, the Java runtime selects the AES-GCM provider and secure random number generator
     * according to its configuration.
     *
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
