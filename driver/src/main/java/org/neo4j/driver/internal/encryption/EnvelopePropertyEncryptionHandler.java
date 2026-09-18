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

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.CryptoContext;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.PropertyEncryptionException;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.util.Futures;

public class EnvelopePropertyEncryptionHandler implements PropertyEncryptionHandler {
    public static final String PROFILE_TYPE = "ENVELOPE";
    public static final long PROFILE_VERSION = 1;

    protected final String profileName;
    private final AsyncEncapsulatedKeyRecordRepository keyRepository;
    protected final AsyncKeyEncapsulationService keyEncapsulationService;
    private final Provider provider;
    private final SecureRandom secureRandomIV;
    private final AEADEncryption aeadEncryption;
    private final KeyCache keyCache;
    private final DriverObservationProvider observationProvider;

    public EnvelopePropertyEncryptionHandler(
            InternalEnvelopePropertyEncryptionProfile profile,
            AEADEncryption aeadEncryption,
            Clock clock,
            DriverObservationProvider observationProvider) {
        Objects.requireNonNull(profile);
        Objects.requireNonNull(clock);
        this.profileName = Objects.requireNonNull(profile.name());
        this.keyEncapsulationService = Objects.requireNonNull(profile.asyncKeyEncapsulationService());
        this.keyRepository = Objects.requireNonNull(profile.asyncKeyRepository());
        this.provider = profile.cryptoContext().map(CryptoContext::provider).orElse(null);
        this.secureRandomIV =
                profile.cryptoContext().map(CryptoContext::ivSecureRandom).orElse(null);
        var aliasCacheConfig = profile.keyAliasIndexConfig().orElse(null);
        var keyCacheConfig = profile.keyCacheConfig().orElse(null);
        this.keyCache = new InMemoryKeyCache(aliasCacheConfig, keyCacheConfig, clock);
        this.aeadEncryption = Objects.requireNonNull(aeadEncryption);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public String profileName() {
        return profileName;
    }

    @Override
    public CompletionStage<byte[]> encrypt(InternalPropertyEncryptionRequest request) {
        return findAndDecapsulateKey(request).thenApply(keyData -> {
            try {
                var encryptedProperty = aeadEncryption.encrypt(
                        request,
                        keyData.key(),
                        keyData.id(),
                        PROFILE_TYPE,
                        PROFILE_VERSION,
                        profileName,
                        provider,
                        secureRandomIV());
                return aeadEncryption.encodeToEncryptedBytes(encryptedProperty);
            } catch (Exception e) {
                throw new PropertyEncryptionException("Error encrypting data: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public CompletionStage<Value> decrypt(
            InternalPropertyDecryptionRequest decryptionRequest, AEADEncryptedProperty encryptedProperty) {
        if (!PROFILE_TYPE.equals(encryptedProperty.profileType())) {
            throw new PropertyEncryptionException(
                    "Unsupported profile type %s".formatted(encryptedProperty.profileType()));
        }
        if (PROFILE_VERSION != encryptedProperty.profileVersion()) {
            throw new PropertyEncryptionException("Unsupported profile version %d for profile type %s"
                    .formatted(encryptedProperty.profileVersion(), encryptedProperty.profileType()));
        }
        return getKey(encryptedProperty.keyId()).thenApply(dek -> {
            try {
                return aeadEncryption.decrypt(encryptedProperty, dek, decryptionRequest.aad, provider);
            } catch (Exception e) {
                throw new PropertyEncryptionException("Error decrypting data: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public AsyncKeyEncapsulationService keyEncapsulationService() {
        return keyEncapsulationService;
    }

    @Override
    public AsyncEncapsulatedKeyRecordRepository keyRepository() {
        return keyRepository;
    }

    @Override
    public KeyCache keyCache() {
        return keyCache;
    }

    private CompletionStage<KeyData> findAndDecapsulateKey(InternalPropertyEncryptionRequest request) {
        try {
            if (request.encryptionKeyId != null) {
                var id = request.encryptionKeyId;
                var cachedKey = keyCache.findById(id);
                if (cachedKey != null) {
                    return CompletableFuture.completedStage(new KeyData(cachedKey.id(), cachedKey.key()));
                } else {
                    var findObservation = observationProvider.encapsulatedKeyRepositoryFindById();
                    return observeAsync(findObservation, () -> keyRepository.findByIdAsync(id))
                            .thenApply(encapsulatedKeyRecord -> {
                                if (encapsulatedKeyRecord == null) {
                                    throw new PropertyEncryptionException("No key found for id %s".formatted(id));
                                }
                                return encapsulatedKeyRecord;
                            })
                            .thenCompose(this::decapsulate);
                }
            } else {
                if (request.encryptionKeyAlias != null) {
                    var alias = request.encryptionKeyAlias;
                    var cachedKey = keyCache.findByAlias(alias);
                    if (cachedKey != null) {
                        return CompletableFuture.completedStage(new KeyData(cachedKey.id(), cachedKey.key()));
                    } else {
                        var findObservation = observationProvider.encapsulatedKeyRepositoryFindByAlias();
                        return observeAsync(findObservation, () -> keyRepository.findByAliasAsync(alias))
                                .thenCompose(encapsulatedKeyRecord -> {
                                    if (encapsulatedKeyRecord == null) {
                                        throw new PropertyEncryptionException(
                                                "No key found for alias %s".formatted(alias));
                                    }
                                    var cachedKeyById = keyCache.findById(encapsulatedKeyRecord.id());
                                    return (cachedKeyById == null)
                                            ? decapsulate(encapsulatedKeyRecord)
                                            : CompletableFuture.completedStage(cachedKeyById);
                                });
                    }
                } else {
                    return CompletableFuture.failedStage(new PropertyEncryptionException(
                            "The encryption request does not have neither key id nor key alias"));
                }
            }
        } catch (Exception e) {
            return CompletableFuture.failedStage(
                    new PropertyEncryptionException("Key repository lookup has failed", e));
        }
    }

    private CompletionStage<KeyData> decapsulate(EncapsulatedKeyRecord encapsulatedKeyRecord) {
        var decapsulateObservation = observationProvider.keyEncapsulationServiceDecapsulate();
        try {
            return observeAsync(
                            decapsulateObservation,
                            () -> keyEncapsulationService.decapsulateAsync(
                                    encapsulatedKeyRecord.encapsulation(), encapsulatedKeyRecord.metadata()))
                    .exceptionally(throwable -> {
                        throwable = Futures.completionExceptionCause(throwable);
                        if (throwable instanceof Neo4jException neo4jException) {
                            throw neo4jException;
                        } else {
                            throw new PropertyEncryptionException("Error decapsulating key", throwable);
                        }
                    })
                    .thenApply(key -> {
                        keyCache.create(
                                encapsulatedKeyRecord.id(),
                                encapsulatedKeyRecord.alias().orElse(null),
                                key);
                        return new KeyData(encapsulatedKeyRecord.id(), key);
                    });
        } catch (Neo4jException e) {
            throw e;
        } catch (Exception e) {
            throw new PropertyEncryptionException("Error decapsulating key: " + e.getMessage(), e);
        }
    }

    private CompletionStage<SecretKey> getKey(String keyId) {
        var cachedKey = keyCache.findById(keyId);
        if (cachedKey != null) {
            return CompletableFuture.completedStage(cachedKey.key());
        } else {
            var findObservation = observationProvider.encapsulatedKeyRepositoryFindById();
            var keyRecordStage = observeAsync(findObservation, () -> keyRepository.findByIdAsync(keyId))
                    .thenApply(encapsulatedKeyRecord -> {
                        if (encapsulatedKeyRecord == null) {
                            throw new PropertyEncryptionException("No key found for id %s".formatted(keyId));
                        }
                        return encapsulatedKeyRecord;
                    });
            return keyRecordStage.thenCompose(encapsulatedKey -> {
                try {
                    var decapsulateObservation = observationProvider.keyEncapsulationServiceDecapsulate();
                    return observeAsync(
                                    decapsulateObservation,
                                    () -> keyEncapsulationService.decapsulateAsync(
                                            encapsulatedKey.encapsulation(), encapsulatedKey.metadata()))
                            .exceptionally(throwable -> {
                                throwable = Futures.completionExceptionCause(throwable);
                                if (throwable instanceof Neo4jException neo4jException) {
                                    throw neo4jException;
                                } else {
                                    throw new PropertyEncryptionException("Error decapsulating key", throwable);
                                }
                            })
                            .thenApply(key -> {
                                keyCache.create(
                                        encapsulatedKey.id(),
                                        encapsulatedKey.alias().orElse(null),
                                        key);
                                return key;
                            });
                } catch (Neo4jException e) {
                    throw e;
                } catch (Exception e) {
                    throw new PropertyEncryptionException("Failed to decapsulate key", e);
                }
            });
        }
    }

    private SecureRandom secureRandomIV() throws NoSuchAlgorithmException {
        return secureRandomIV == null ? SecureRandom.getInstanceStrong() : secureRandomIV;
    }
}
