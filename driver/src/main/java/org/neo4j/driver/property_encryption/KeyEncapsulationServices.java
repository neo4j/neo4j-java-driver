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
package org.neo4j.driver.property_encryption;

import java.security.NoSuchAlgorithmException;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.property_encryption.LocalKeyEncapsulationService;
import org.neo4j.driver.util.Preview;

/**
 * A factory for {@link KeyEncapsulationService} implementation provided with the driver.
 * <p>
 * Note that additional implementations are possible.
 * @see KeyEncapsulationService
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public final class KeyEncapsulationServices {
    private KeyEncapsulationServices() {}

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that uses the provided AES-256 {@link SecretKey}
     * as a master key for encrypting and decrypting data keys.
     * @param masterKey the master key
     * @return the new implementation
     * @throws NoSuchAlgorithmException if no AES algorithm is found
     */
    public static KeyEncapsulationService local(SecretKey masterKey) throws NoSuchAlgorithmException {
        return new LocalKeyEncapsulationService(masterKey);
    }
}
