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

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import org.bouncycastle.jcajce.interfaces.MLKEMPrivateKey;
import org.bouncycastle.jcajce.interfaces.MLKEMPublicKey;
import org.bouncycastle.jcajce.spec.MLKEMParameterSpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;

final class KeyEncapsulationServiceUtil {
    static AsyncKeyEncapsulationService create() {
        var provider = new BouncyCastleProvider();
        KeyPairGenerator kpg;
        try {
            kpg = KeyPairGenerator.getInstance("ML-KEM", provider);
            kpg.initialize(MLKEMParameterSpec.ml_kem_768);
        } catch (InvalidAlgorithmParameterException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        var keyPair = kpg.generateKeyPair();
        var publicKey = (MLKEMPublicKey) keyPair.getPublic();
        var privateKey = (MLKEMPrivateKey) keyPair.getPrivate();
        return new MLKEMEncapsulationService(publicKey, privateKey, provider);
    }
}
