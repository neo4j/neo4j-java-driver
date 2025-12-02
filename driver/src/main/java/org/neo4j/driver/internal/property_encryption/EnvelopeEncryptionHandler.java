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

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;

public class EnvelopeEncryptionHandler implements EncryptionHandler {
    protected final String profileName;
    private final PropertyEncryptionProfile.KeyReference defaultKeyReference;
    private final PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository;
    private final EncapsulatedKeyManager keyManager;
    protected final KeyEncapsulationService keyEncapsulationService;
    private final AEADEncryption aeadEncryption;

    public EnvelopeEncryptionHandler(
            String profileName,
            PropertyEncryptionProfile.KeyReference defaultKeyReference,
            KeyEncapsulationService keyEncapsulationService,
            PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository,
            ValueFactory valueFactory,
            @SuppressWarnings("deprecation") Logging logging) {
        this.profileName = Objects.requireNonNull(profileName);
        this.keyEncapsulationService = Objects.requireNonNull(keyEncapsulationService);
        this.defaultKeyReference = Objects.requireNonNull(defaultKeyReference);
        this.keyRepository = Objects.requireNonNull(keyRepository);
        this.keyManager = new InternalEncapsulatedKeyManager(keyEncapsulationService, keyRepository);
        this.aeadEncryption = new AEADEncryption(valueFactory, logging);
    }

    @Override
    public String profileName() {
        return profileName;
    }

    @Override
    public CompletionStage<byte[]> encrypt(InternalPropertyEncryptRequest request) {
        return getKeyData(request).thenApply(keyData -> {
            try {
                var encryptedProperty = aeadEncryption.encrypt(request, keyData.key(), keyData.keyId(), profileName);
                return aeadEncryption.packAEAD(encryptedProperty);
            } catch (Exception e) {
                throw new ClientException("Error encrypting data: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public CompletionStage<Value> decrypt(
            InternalPropertyDecryptRequest decryptionRequest, AEADEncryptedProperty encryptedProperty) {
        return getKey(encryptedProperty.keyId()).thenApply(dek -> {
            try {
                return aeadEncryption.decrypt(encryptedProperty, dek, decryptionRequest.aad);
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
    public PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository() {
        return keyRepository;
    }

    public EncapsulatedKeyManager keyManager() {
        return keyManager;
    }

    private CompletionStage<KeyData> getKeyData(InternalPropertyEncryptRequest request) {
        var keyStage = request.encryptionKeyId != null
                ? keyRepository.findById(request.encryptionKeyId)
                : request.encryptionKeyAlias != null
                        ? keyRepository.findByAlias(request.encryptionKeyAlias)
                        : switch (defaultKeyReference.type()) {
                            case ID -> keyRepository.findById(defaultKeyReference.reference());
                            case ALIAS -> keyRepository.findByAlias(defaultKeyReference.reference());
                        };
        return keyStage.thenCompose(encapsulatedKey -> {
            if (encapsulatedKey == null) {
                throw new ClientException("No encapsulated key found");
            }
            return keyEncapsulationService
                    .decapsulate(encapsulatedKey.encapsulation(), encapsulatedKey.metadata())
                    .thenApply(key -> new KeyData(key, encapsulatedKey.id()));
        });
    }

    private CompletionStage<SecretKey> getKey(String keyId) {
        // TODO handle null key
        return keyRepository
                .findById(keyId)
                .thenCompose(encapsulatedKey -> keyEncapsulationService.decapsulate(
                        encapsulatedKey.encapsulation(), encapsulatedKey.metadata()));
    }

    private record KeyData(SecretKey key, String keyId) {}
}
