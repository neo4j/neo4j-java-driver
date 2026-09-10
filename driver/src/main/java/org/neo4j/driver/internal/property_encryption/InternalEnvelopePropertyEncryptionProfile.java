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
package org.neo4j.driver.internal.property_encryption;

import java.security.Provider;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.property_encryption.CacheConfig;
import org.neo4j.driver.property_encryption.CryptoContext;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public record InternalEnvelopePropertyEncryptionProfile(
        String name,
        KeyEncapsulationService keyEncapsulationService,
        EncapsulatedKeyRecordRepository keyRepository,
        CryptoContext cryptoContextRef,
        CacheConfig keyCacheConfigRef,
        CacheConfig keyAliasCacheConfigRef)
        implements EnvelopePropertyEncryptionProfile {
    public InternalEnvelopePropertyEncryptionProfile {
        Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        Objects.requireNonNull(keyEncapsulationService);
        Objects.requireNonNull(keyRepository);
    }

    @Override
    public Optional<CryptoContext> cryptoContext() {
        return Optional.ofNullable(cryptoContextRef);
    }

    @Override
    public Optional<CacheConfig> keyCacheConfig() {
        return Optional.ofNullable(keyCacheConfigRef);
    }

    @Override
    public Optional<CacheConfig> keyAliasIndexConfig() {
        return Optional.ofNullable(keyAliasCacheConfigRef);
    }

    public static class Builder implements EnvelopePropertyEncryptionProfile.Builder {
        final String name;
        final KeyEncapsulationService keyEncapsulationService;
        final EncapsulatedKeyRecordRepository keyRepository;
        CryptoContext cryptoContextRef;
        CacheConfig keyCacheConfigRef = new CacheConfigRecord(100, Duration.ofMinutes(15));
        CacheConfig keyAliasCacheConfigRef = new CacheConfigRecord(100, Duration.ofSeconds(15));

        public Builder(
                String name,
                KeyEncapsulationService keyEncapsulationService,
                EncapsulatedKeyRecordRepository keyRepository) {
            this.name = Objects.requireNonNull(name);
            if (name.isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            this.keyEncapsulationService = Objects.requireNonNull(keyEncapsulationService);
            this.keyRepository = Objects.requireNonNull(keyRepository);
        }

        @Override
        public Builder withCryptoContext(Provider provider, SecureRandom ivSecureRandom) {
            this.cryptoContextRef = new CryptoContextRecord(provider, ivSecureRandom);
            return this;
        }

        @Override
        public Builder withKeyCache(int maxSize, Duration ttl) {
            this.keyCacheConfigRef = new CacheConfigRecord(maxSize, ttl);
            return this;
        }

        @Override
        public Builder withoutKeyCache() {
            this.keyCacheConfigRef = null;
            return this;
        }

        @Override
        public Builder withKeyAliasIndex(int maxSize, Duration ttl) {
            this.keyAliasCacheConfigRef = new CacheConfigRecord(maxSize, ttl);
            return this;
        }

        @Override
        public Builder withoutKeyAliasIndex() {
            this.keyAliasCacheConfigRef = null;
            return this;
        }

        @Override
        public EnvelopePropertyEncryptionProfile build() {
            return new InternalEnvelopePropertyEncryptionProfile(
                    name,
                    keyEncapsulationService,
                    keyRepository,
                    cryptoContextRef,
                    keyCacheConfigRef,
                    keyAliasCacheConfigRef);
        }
    }
}
