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
package org.neo4j.driver.encryption;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Objects;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.encryption.LocalKeyEncapsulationService;
import org.neo4j.driver.util.Preview;

/**
 * A factory for {@link KeyEncapsulationService} implementations.
 * <p>
 * Implementations are not limited to those provided by this factory.
 *
 * @see KeyEncapsulationService
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public final class KeyEncapsulationServices {
    private KeyEncapsulationServices() {}

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that uses the provided AES-256 {@link SecretKey} as
     * a master key for encapsulating and decapsulating data keys.
     * <p>
     * The encapsulation uses AES-GCM ({@literal "AES/GCM/NoPadding"}) with the provided master key. The resulting
     * encapsulation contains a 256-bit AES data encryption key protected by the master key, together with a 96-bit
     * (12-byte) initialization vector (IV) and a 128-bit (16-byte) authentication tag.
     * <p>
     * The Java runtime determines and provides the {@link Provider} and {@link SecureRandom} according to its
     * configuration. The provider is used for AES key generation and AES-GCM operations, while the {@link SecureRandom}
     * is used as the source of initialization vectors.
     *
     * @param masterKey the AES-256 master key, must not be {@literal null}
     * @return the new key encapsulation service
     * @throws NoSuchAlgorithmException if the required AES algorithm is not available
     */
    public static KeyEncapsulationService local(SecretKey masterKey) throws NoSuchAlgorithmException {
        return new LocalKeyEncapsulationService(masterKey, null, null);
    }

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that uses the provided AES-256 {@link SecretKey} as
     * a master key for encapsulating and decapsulating data keys.
     * <p>
     * The encapsulation uses AES-GCM ({@literal "AES/GCM/NoPadding"}) with the provided master key. The resulting
     * encapsulation contains a 256-bit AES data encryption key protected by the master key, together with a 96-bit
     * (12-byte) initialization vector (IV), sourced from the provided {@link SecureRandom}, and a 128-bit (16-byte)
     * authentication tag.
     * <p>
     * The supplied {@link Provider} is used for AES key generation and AES-GCM operations. The supplied
     * {@link SecureRandom} is used as the source of initialization vectors.
     *
     * @param masterKey      the AES-256 master key, must not be {@literal null}
     * @param provider       the {@link Provider} to use for cryptographic operations, must not be {@literal null}
     * @param ivSecureRandom the {@link SecureRandom} to use for IV generation, must not be {@literal null} and
     *                       {@link SecureRandom#getProvider()} must resolve to the provider parameter
     * @return the new key encapsulation service
     * @throws NoSuchAlgorithmException if the required AES algorithm is not available
     */
    public static KeyEncapsulationService local(SecretKey masterKey, Provider provider, SecureRandom ivSecureRandom)
            throws NoSuchAlgorithmException {
        Objects.requireNonNull(provider);
        Objects.requireNonNull(ivSecureRandom);
        if (ivSecureRandom.getProvider() != provider) {
            throw new IllegalArgumentException("SecureRandom must use the supplied provider");
        }
        return new LocalKeyEncapsulationService(masterKey, provider, ivSecureRandom);
    }
}
