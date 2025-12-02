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
package org.neo4j.driver.property_encryption.azure_keyvault;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyAsyncClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public final class AzureKeyEncapsulationService implements KeyEncapsulationService {
    private final KeyGenerator keyGenerator;
    private final AzureEncapsulationOptions defaultOptions;

    public AzureKeyEncapsulationService(AzureEncapsulationOptions defaultOptions) throws NoSuchAlgorithmException {
        this.defaultOptions = Objects.requireNonNull(defaultOptions);
        this.keyGenerator = KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256);
    }

    @Override
    public CompletionStage<EncapsulationResult> encapsulate(KeyEncapsulationOptions options) {
        var encapsulationOptions = Objects.requireNonNullElse(options, defaultOptions);
        var kmsKeyId = ((AzureEncapsulationOptions) encapsulationOptions).keyId();
        var key = keyGenerator.generateKey();
        return forKey(kmsKeyId)
                .encrypt(EncryptionAlgorithm.RSA_OAEP, key.getEncoded())
                .toFuture()
                .thenApply(encryptResult ->
                        EncapsulationResult.of(encryptResult.getCipherText(), encapsulationOptions, key));
    }

    @Override
    public CompletionStage<SecretKey> decapsulate(byte[] ciphertext, Map<String, String> metadata) {
        var options = AzureEncapsulationOptions.of(metadata);
        return forKey(options.keyId())
                .decrypt(EncryptionAlgorithm.RSA_OAEP, ciphertext)
                .toFuture()
                .thenApply(decryptResult -> new SecretKeySpec(decryptResult.getPlainText(), "AES"));
    }

    private static CryptographyAsyncClient forKey(String keyVaultKeyId) {
        return new CryptographyClientBuilder()
                .credential(new DefaultAzureCredentialBuilder().build())
                .keyIdentifier(keyVaultKeyId)
                .buildAsyncClient();
    }
}
