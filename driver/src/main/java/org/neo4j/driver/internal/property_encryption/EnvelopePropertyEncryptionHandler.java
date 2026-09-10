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
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.property_encryption.CryptoContext;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public class EnvelopePropertyEncryptionHandler implements PropertyEncryptionHandler {
    protected final String profileName;
    private final EncapsulatedKeyRecordRepository keyRepository;
    protected final KeyEncapsulationService keyEncapsulationService;
    private final Provider provider;
    private final SecureRandom secureRandomIV;
    private final AEADEncryption aeadEncryption;
    private final KeyCache keyCache;
    private final DriverObservationProvider observationProvider;

    public EnvelopePropertyEncryptionHandler(
            EnvelopePropertyEncryptionProfile profile,
            AEADEncryption aeadEncryption,
            Clock clock,
            DriverObservationProvider observationProvider) {
        Objects.requireNonNull(profile);
        Objects.requireNonNull(clock);
        this.profileName = Objects.requireNonNull(profile.name());
        this.keyEncapsulationService = Objects.requireNonNull(profile.keyEncapsulationService());
        this.keyRepository = Objects.requireNonNull(profile.keyRepository());
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
                        request, keyData.key(), keyData.id(), profileName, provider, secureRandomIV());
                return aeadEncryption.encodeToEncryptedBytes(encryptedProperty);
            } catch (Exception e) {
                throw new ClientException("Error encrypting data: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public CompletionStage<Value> decrypt(
            InternalPropertyDecryptionRequest decryptionRequest, AEADEncryptedProperty encryptedProperty) {
        return getKey(encryptedProperty.keyId()).thenApply(dek -> {
            try {
                return aeadEncryption.decrypt(encryptedProperty, dek, decryptionRequest.aad, provider);
            } catch (Exception e) {
                throw new ClientException("Error decrypting data: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public KeyEncapsulationService keyEncapsulationService() {
        return keyEncapsulationService;
    }

    @Override
    public EncapsulatedKeyRecordRepository keyRepository() {
        return keyRepository;
    }

    @Override
    public KeyCache keyCache() {
        return keyCache;
    }

    private CompletionStage<KeyData> findAndDecapsulateKey(InternalPropertyEncryptionRequest request) {
        CompletionStage<EncapsulatedKeyRecord> keyRecordStage;
        try {
            if (request.encryptionKeyId != null) {
                var id = request.encryptionKeyId;
                var cachedKey = keyCache.findById(id);
                if (cachedKey != null) {
                    return CompletableFuture.completedStage(new KeyData(cachedKey.id(), cachedKey.key()));
                } else {
                    var findObservation = observationProvider.encapsulatedKeyRepositoryFindById();
                    keyRecordStage = observeAsync(findObservation, () -> keyRepository.findById(id))
                            .thenApply(encapsulatedKeyRecord -> {
                                if (encapsulatedKeyRecord == null) {
                                    throw new ClientException("No key found for id %s".formatted(id));
                                }
                                return encapsulatedKeyRecord;
                            });
                }
            } else {
                if (request.encryptionKeyAlias != null) {
                    var alias = request.encryptionKeyAlias;
                    var cachedKey = keyCache.findByAlias(alias);
                    if (cachedKey != null) {
                        return CompletableFuture.completedStage(new KeyData(cachedKey.id(), cachedKey.key()));
                    } else {
                        var findObservation = observationProvider.encapsulatedKeyRepositoryFindByAlias();
                        keyRecordStage = observeAsync(
                                        findObservation, () -> keyRepository.findByAlias(request.encryptionKeyAlias))
                                .thenApply(encapsulatedKeyRecord -> {
                                    // TODO decide if key cache lookup by id is sufficient
                                    if (encapsulatedKeyRecord == null) {
                                        throw new ClientException(
                                                "No key found for alias %s".formatted(request.encryptionKeyAlias));
                                    }
                                    return encapsulatedKeyRecord;
                                });
                    }
                } else {
                    keyRecordStage = CompletableFuture.failedStage(
                            new ClientException("The encryption request does not have neither key id nor key alias"));
                }
            }
        } catch (Exception e) {
            keyRecordStage = CompletableFuture.failedStage(new ClientException("Key repository lookup has failed", e));
        }

        return keyRecordStage.thenCompose(encapsulatedKey -> {
            try {
                var decapsulateObservation = observationProvider.keyEncapsulationServiceDecapsulate();
                return observeAsync(
                                decapsulateObservation,
                                () -> keyEncapsulationService.decapsulate(
                                        encapsulatedKey.encapsulation(), encapsulatedKey.metadata()))
                        .thenApply(key -> {
                            keyCache.create(
                                    encapsulatedKey.id(),
                                    encapsulatedKey.alias().orElse(null),
                                    key);
                            return new KeyData(encapsulatedKey.id(), key);
                        });
            } catch (Exception e) {
                throw new ClientException("Failed to decapsulate key", e);
            }
        });
    }

    private CompletionStage<SecretKey> getKey(String keyId) {
        var cachedKey = keyCache.findById(keyId);
        if (cachedKey != null) {
            return CompletableFuture.completedStage(cachedKey.key());
        } else {
            var findObservation = observationProvider.encapsulatedKeyRepositoryFindById();
            var keyRecordStage = observeAsync(findObservation, () -> keyRepository.findById(keyId))
                    .thenApply(encapsulatedKeyRecord -> {
                        if (encapsulatedKeyRecord == null) {
                            throw new ClientException("No key found for id %s".formatted(keyId));
                        }
                        return encapsulatedKeyRecord;
                    });
            return keyRecordStage.thenCompose(encapsulatedKey -> {
                try {
                    var decapsulateObservation = observationProvider.keyEncapsulationServiceDecapsulate();
                    return observeAsync(
                                    decapsulateObservation,
                                    () -> keyEncapsulationService.decapsulate(
                                            encapsulatedKey.encapsulation(), encapsulatedKey.metadata()))
                            .thenApply(key -> {
                                keyCache.create(
                                        encapsulatedKey.id(),
                                        encapsulatedKey.alias().orElse(null),
                                        key);
                                return key;
                            });
                } catch (Exception e) {
                    throw new ClientException("Failed to decapsulate key", e);
                }
            });
        }
    }

    private SecureRandom secureRandomIV() throws NoSuchAlgorithmException {
        return secureRandomIV == null ? SecureRandom.getInstanceStrong() : secureRandomIV;
    }
}
