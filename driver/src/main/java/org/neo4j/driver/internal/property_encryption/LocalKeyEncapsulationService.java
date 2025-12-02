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

import static javax.crypto.Cipher.DECRYPT_MODE;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public final class LocalKeyEncapsulationService implements KeyEncapsulationService {
    private final SecretKey masterKey;
    private final KeyGenerator keyGenerator;

    public LocalKeyEncapsulationService(SecretKey masterKey) throws NoSuchAlgorithmException {
        this.masterKey = Objects.requireNonNull(masterKey);
        this.keyGenerator = KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256);
    }

    @Override
    public CompletionStage<EncapsulationResult> encapsulate(KeyEncapsulationOptions options) {
        var dek = keyGenerator.generateKey();
        return CompletableFuture.supplyAsync(() -> {
            try {
                var cipher = Cipher.getInstance("AES/GCM/NoPadding");
                var iv = SecureRandom.getInstanceStrong().generateSeed(12);
                var gcmSpec = new GCMParameterSpec(128, iv);

                cipher.init(Cipher.ENCRYPT_MODE, masterKey, gcmSpec);
                var encryptedDek = cipher.doFinal(dek.getEncoded());

                return EncapsulationResult.of(
                        encryptedDek, () -> Map.of("iv", Base64.getEncoder().encodeToString(iv)), dek);
            } catch (Exception e) {
                throw new ClientException("Failed to encrypt data", e);
            }
        });
    }

    @Override
    public CompletionStage<SecretKey> decapsulate(byte[] encapsulation, Map<String, String> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            var iv = Base64.getDecoder().decode(metadata.get("iv"));
            try {
                var cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(DECRYPT_MODE, masterKey, new GCMParameterSpec(128, iv));

                var keyBytes = cipher.doFinal(encapsulation);
                return new SecretKeySpec(keyBytes, "AES");
            } catch (Exception e) {
                throw new ClientException("Failed to decrypt data", e);
            }
        });
    }
}
