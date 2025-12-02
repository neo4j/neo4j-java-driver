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
package org.neo4j.driver.it.encryption.local;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import org.bouncycastle.pqc.jcajce.interfaces.KyberPrivateKey;
import org.bouncycastle.pqc.jcajce.interfaces.KyberPublicKey;
import org.bouncycastle.pqc.jcajce.provider.BouncyCastlePQCProvider;
import org.bouncycastle.pqc.jcajce.spec.KyberParameterSpec;
import org.neo4j.driver.it.encryption.common.AbstractEnvelopeEncryptionIT;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.kyber.KyberEncapsulationService;

class EnvelopeEncryptionIT extends AbstractEnvelopeEncryptionIT {

    @Override
    protected KeyEncapsulationService keyEncapsulationService() throws NoSuchAlgorithmException {
        // 1. Add Bouncy Castle PQC provider
        Security.addProvider(new BouncyCastlePQCProvider());

        // 2. Initialize KeyPairGenerator for Kyber (choose kyber512, kyber768, kyber1024)
        KeyPairGenerator kpg;
        try {
            kpg = KeyPairGenerator.getInstance("Kyber", "BCPQC");
            kpg.initialize(KyberParameterSpec.kyber768); // example
        } catch (NoSuchProviderException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }

        // 3. Generate key pair
        var keyPair = kpg.generateKeyPair();

        // 4. Extract typed Kyber keys
        var publicKey = (KyberPublicKey) keyPair.getPublic();
        var privateKey = (KyberPrivateKey) keyPair.getPrivate();
        return new KyberEncapsulationService(publicKey, privateKey);
    }
}
