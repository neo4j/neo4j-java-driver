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
package org.neo4j.driver.it.encryption.ml_kem;

import java.security.Provider;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;
import org.bouncycastle.jcajce.SecretKeyWithEncapsulation;
import org.bouncycastle.jcajce.interfaces.MLKEMPrivateKey;
import org.bouncycastle.jcajce.interfaces.MLKEMPublicKey;
import org.bouncycastle.jcajce.spec.KEMExtractSpec;
import org.bouncycastle.jcajce.spec.KEMGenerateSpec;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationResult;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.exceptions.PropertyEncryptionException;

public final class MLKEMEncapsulationService implements AsyncKeyEncapsulationService {
    private static final int AES_256_KEY_SIZE = 32;
    private static final String ALGORITHM = "ML-KEM";
    private static final String KEY_ALGORITHM = "AES";

    private final MLKEMPublicKey publicKey;
    private final MLKEMPrivateKey privateKey;
    private final Provider provider;
    private final byte[] info;

    public MLKEMEncapsulationService(MLKEMPublicKey publicKey, MLKEMPrivateKey privateKey, Provider provider) {
        this.publicKey = Objects.requireNonNull(publicKey);
        this.privateKey = Objects.requireNonNull(privateKey);
        this.provider = Objects.requireNonNull(provider);
        this.info = "aes-256-key".getBytes();
    }

    @Override
    public CompletionStage<KeyEncapsulationResult> encapsulateAsync(KeyEncapsulationOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var kg = KeyGenerator.getInstance(ALGORITHM, provider);
                var genSpec = new KEMGenerateSpec(publicKey, KEY_ALGORITHM, AES_256_KEY_SIZE * 8);
                kg.init(genSpec);
                var skwe = (SecretKeyWithEncapsulation) kg.generateKey();
                var shared = skwe.getEncoded();
                try {
                    var aes = deriveAesKey(shared);
                    var encapsulation = skwe.getEncapsulation();
                    return KeyEncapsulationResult.of(encapsulation, Map.of(), aes);
                } finally {
                    zeroize(shared);
                }
            } catch (Exception e) {
                throw new PropertyEncryptionException("Failed to encapsulate with ML-KEM", e);
            }
        });
    }

    @Override
    public CompletionStage<SecretKey> decapsulateAsync(byte[] encapsulation, Map<String, String> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var kg = KeyGenerator.getInstance(ALGORITHM, provider);
                var extSpec = new KEMExtractSpec(privateKey, encapsulation, KEY_ALGORITHM);
                kg.init(extSpec);
                var skwe = (SecretKeyWithEncapsulation) kg.generateKey();
                var shared = skwe.getEncoded();
                try {
                    return deriveAesKey(shared);
                } finally {
                    zeroize(shared);
                }
            } catch (Exception e) {
                throw new PropertyEncryptionException("Failed to decapsulate ML-KEM key", e);
            }
        });
    }

    private SecretKey deriveAesKey(byte[] secret) {
        var keyBytes = hkdfSha256(secret, info, AES_256_KEY_SIZE);
        return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
    }

    private static byte[] hkdfSha256(byte[] ikm, byte[] info, int length) {
        var hkdf = new HKDFBytesGenerator(new SHA256Digest());
        hkdf.init(new HKDFParameters(ikm, null, info));
        var out = new byte[length];
        hkdf.generateBytes(out, 0, out.length);
        return out;
    }

    private static void zeroize(byte[] data) {
        if (data != null) Arrays.fill(data, (byte) 0);
    }
}
