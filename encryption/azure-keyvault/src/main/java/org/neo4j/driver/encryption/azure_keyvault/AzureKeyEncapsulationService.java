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
package org.neo4j.driver.encryption.azure_keyvault;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyAsyncClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationResult;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;

final class AzureKeyEncapsulationService implements AsyncKeyEncapsulationService {
    private final KeyGenerator keyGenerator;
    private final AzureEncapsulationOptions defaultOptions;
    private final TokenCredential credential;
    private final CryptographyClientCache clientCache;

    AzureKeyEncapsulationService(AzureEncapsulationOptions defaultOptions, Provider provider, int clientCacheMaxSize)
            throws NoSuchAlgorithmException {
        this.defaultOptions = Objects.requireNonNull(defaultOptions);
        this.keyGenerator =
                provider != null ? KeyGenerator.getInstance("AES", provider) : KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256);
        this.credential = new DefaultAzureCredentialBuilder().build();
        this.clientCache = new CryptographyClientCache(clientCacheMaxSize);
    }

    @Override
    public CompletionStage<KeyEncapsulationResult> encapsulateAsync(KeyEncapsulationOptions options) {
        var encapsulationOptions = Objects.requireNonNullElse(options, defaultOptions);
        var kmsKeyId = ((AzureEncapsulationOptionsImpl) encapsulationOptions).keyId();
        var key = keyGenerator.generateKey();
        return clientFor(kmsKeyId)
                .encrypt(EncryptionAlgorithm.RSA_OAEP, key.getEncoded())
                .toFuture()
                .thenApply(encryptResult ->
                        KeyEncapsulationResult.of(encryptResult.getCipherText(), encapsulationOptions.toMap(), key));
    }

    @Override
    public CompletionStage<SecretKey> decapsulateAsync(byte[] ciphertext, Map<String, String> metadata) {
        var options = AzureEncapsulationOptionsImpl.of(metadata);
        return clientFor(options.keyId())
                .decrypt(EncryptionAlgorithm.RSA_OAEP, ciphertext)
                .toFuture()
                .thenApply(decryptResult -> new SecretKeySpec(decryptResult.getPlainText(), "AES"));
    }

    private CryptographyAsyncClient clientFor(String keyId) {
        return clientCache.get(keyId, id -> new CryptographyClientBuilder()
                .credential(credential)
                .keyIdentifier(id)
                .buildAsyncClient());
    }
}
