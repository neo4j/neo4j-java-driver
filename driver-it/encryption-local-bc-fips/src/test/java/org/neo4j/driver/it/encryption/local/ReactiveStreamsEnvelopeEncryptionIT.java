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

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import javax.crypto.KeyGenerator;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.neo4j.driver.it.encryption.common.AbstractReactiveStreamsEnvelopeEncryptionIT;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.KeyEncapsulationServices;

final class ReactiveStreamsEnvelopeEncryptionIT extends AbstractReactiveStreamsEnvelopeEncryptionIT {
    @Override
    protected KeyEncapsulationService keyEncapsulationService() throws NoSuchAlgorithmException {
        var provider = provider();
        var keyGenerator = KeyGenerator.getInstance("AES", provider);
        keyGenerator.init(256);
        var masterKey = keyGenerator.generateKey();
        return KeyEncapsulationServices.local(masterKey, provider, secureRandomIV(provider));
    }

    @Override
    protected Provider provider() {
        return new BouncyCastleFipsProvider();
    }

    @Override
    protected SecureRandom secureRandomIV(Provider provider) throws NoSuchAlgorithmException {
        return SecureRandom.getInstance("NONCEANDIV", provider);
    }
}
