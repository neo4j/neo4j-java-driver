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
package org.neo4j.driver.internal.encryption;

import java.security.Provider;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.encryption.BaseEncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.BaseKeyEncapsulationService;
import org.neo4j.driver.encryption.CacheConfig;
import org.neo4j.driver.encryption.CryptoContext;
import org.neo4j.driver.encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.encryption.KeyEncapsulationService;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.internal.encryption.async.DelegatingAsyncKeyEncapsulationService;
import org.neo4j.driver.internal.encryption.async.DelegatingEncapsulatedKeyRecordRepository;

public record InternalEnvelopePropertyEncryptionProfile(
        String name,
        BaseKeyEncapsulationService keyEncapsulationService,
        AsyncKeyEncapsulationService asyncKeyEncapsulationService,
        BaseEncapsulatedKeyRecordRepository keyRepository,
        AsyncEncapsulatedKeyRecordRepository asyncKeyRepository,
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
        Objects.requireNonNull(asyncKeyEncapsulationService);
        Objects.requireNonNull(keyRepository);
        Objects.requireNonNull(asyncKeyRepository);
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
        final BaseKeyEncapsulationService keyEncapsulationService;
        final AsyncKeyEncapsulationService asyncKeyEncapsulationService;
        final BaseEncapsulatedKeyRecordRepository keyRepository;
        final AsyncEncapsulatedKeyRecordRepository asyncKeyRepository;
        CryptoContext cryptoContextRef;
        CacheConfig keyCacheConfigRef = new CacheConfigRecord(100, Duration.ofMinutes(15));
        CacheConfig keyAliasCacheConfigRef = new CacheConfigRecord(100, Duration.ofSeconds(15));

        public Builder(
                String name,
                BaseKeyEncapsulationService keyEncapsulationService,
                BaseEncapsulatedKeyRecordRepository keyRepository) {
            Objects.requireNonNull(name);
            if (name.isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            this.name = name;
            this.keyEncapsulationService = Objects.requireNonNull(keyEncapsulationService);
            if (keyEncapsulationService instanceof AsyncKeyEncapsulationService async) {
                this.asyncKeyEncapsulationService = async;
            } else if (keyEncapsulationService instanceof KeyEncapsulationService sync) {
                this.asyncKeyEncapsulationService = new DelegatingAsyncKeyEncapsulationService(sync, sync.executor());
            } else {
                throw new IllegalArgumentException("Unsupported key encapsulation service type: %s"
                        .formatted(keyEncapsulationService.getClass().getName()));
            }
            this.keyRepository = Objects.requireNonNull(keyRepository);
            if (keyRepository instanceof AsyncEncapsulatedKeyRecordRepository async) {
                this.asyncKeyRepository = async;
            } else if (keyRepository instanceof EncapsulatedKeyRecordRepository sync) {
                this.asyncKeyRepository = new DelegatingEncapsulatedKeyRecordRepository(sync, sync.executor());
            } else {
                throw new IllegalArgumentException("Unsupported key repository type: %s"
                        .formatted(keyRepository.getClass().getName()));
            }
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
                    asyncKeyEncapsulationService,
                    keyRepository,
                    asyncKeyRepository,
                    cryptoContextRef,
                    keyCacheConfigRef,
                    keyAliasCacheConfigRef);
        }
    }
}
