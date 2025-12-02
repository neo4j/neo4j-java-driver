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
package org.neo4j.driver.property_encryption.kyber;

import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;
import org.bouncycastle.jcajce.SecretKeyWithEncapsulation;
import org.bouncycastle.jcajce.spec.KEMExtractSpec;
import org.bouncycastle.jcajce.spec.KEMGenerateSpec;
import org.bouncycastle.pqc.jcajce.interfaces.KyberPrivateKey;
import org.bouncycastle.pqc.jcajce.interfaces.KyberPublicKey;
import org.bouncycastle.pqc.jcajce.provider.BouncyCastlePQCProvider;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public final class KyberEncapsulationService implements KeyEncapsulationService {

    static {
        Security.addProvider(new BouncyCastlePQCProvider());
    }

    private static final int AES_256_KEY_SIZE = 32;

    private final KyberPublicKey publicKey;
    private final KyberPrivateKey privateKey;
    private final SecureRandom secureRandom;
    private final byte[] info;

    public KyberEncapsulationService(KyberPublicKey publicKey, KyberPrivateKey privateKey) {
        if (publicKey == null || privateKey == null) {
            throw new IllegalArgumentException("Both public and private keys are required");
        }
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.secureRandom = new SecureRandom();
        this.info = "aes-256-key".getBytes();
    }

    @Override
    public CompletionStage<EncapsulationResult> encapsulate(KeyEncapsulationOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 🔹 Create generator for Kyber KEM
                javax.crypto.KeyGenerator kg = javax.crypto.KeyGenerator.getInstance("Kyber", "BCPQC");

                // 🔹 Build generation spec (recipient public key + info)
                KEMGenerateSpec genSpec = new KEMGenerateSpec(publicKey, "AES", AES_256_KEY_SIZE * 8);

                // 🔹 Initialize generator with spec
                kg.init(genSpec);

                // 🔹 Generate the key (returns SecretKeyWithEncapsulation)
                SecretKeyWithEncapsulation skwe = (SecretKeyWithEncapsulation) kg.generateKey();

                // 🔹 Derive AES key from shared secret
                byte[] shared = skwe.getEncoded();
                SecretKey aes = deriveAesKey(shared);
                zeroize(shared);

                // 🔹 Return encapsulation bytes and AES key
                byte[] encapsulation = skwe.getEncapsulation();
                return EncapsulationResult.of(encapsulation, Map::of, aes);
            } catch (Exception e) {
                throw new ClientException("Failed to encapsulate with Kyber", e);
            }
        });
    }

    @Override
    public CompletionStage<SecretKey> decapsulate(byte[] encapsulation, Map<String, String> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 🔹 Create generator for Kyber KEM
                javax.crypto.KeyGenerator kg = javax.crypto.KeyGenerator.getInstance("Kyber", "BCPQC");

                // 🔹 Build extraction spec (private key + encapsulation + info)
                KEMExtractSpec extSpec = new KEMExtractSpec(privateKey, encapsulation, "AES");

                // 🔹 Initialize generator with extraction spec
                kg.init(extSpec);

                // 🔹 Generate the key (returns SecretKeyWithEncapsulation)
                SecretKeyWithEncapsulation skwe = (SecretKeyWithEncapsulation) kg.generateKey();

                // 🔹 Derive AES key from shared secret
                byte[] shared = skwe.getEncoded();
                SecretKey aes = deriveAesKey(shared);
                zeroize(shared);
                return new SecretKeySpec(aes.getEncoded(), "AES");
            } catch (Exception e) {
                throw new RuntimeException("Failed to decapsulate Kyber key", e);
            }
        });
    }

    private SecretKey deriveAesKey(byte[] secret) {
        byte[] keyBytes = hkdfSha256(secret, info, AES_256_KEY_SIZE);
        return new SecretKeySpec(keyBytes, "AES");
    }

    private static byte[] hkdfSha256(byte[] ikm, byte[] info, int length) {
        HKDFBytesGenerator hkdf = new HKDFBytesGenerator(new SHA256Digest());
        hkdf.init(new HKDFParameters(ikm, null, info));
        byte[] out = new byte[length];
        hkdf.generateBytes(out, 0, out.length);
        return out;
    }

    private static void zeroize(byte[] data) {
        if (data != null) Arrays.fill(data, (byte) 0);
    }
}
