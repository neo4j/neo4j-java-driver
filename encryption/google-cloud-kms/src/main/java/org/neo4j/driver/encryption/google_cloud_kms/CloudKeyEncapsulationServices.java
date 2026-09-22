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
package org.neo4j.driver.encryption.google_cloud_kms;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Objects;
import org.neo4j.driver.encryption.BaseKeyEncapsulationService;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.util.Preview;

/**
 * A factory for {@link BaseKeyEncapsulationService} implementations using Google Cloud KMS.
 *
 * @see BaseKeyEncapsulationService
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public final class CloudKeyEncapsulationServices {
    private CloudKeyEncapsulationServices() {}

    /**
     * Returns a new {@link AsyncKeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Google Cloud KMS to encapsulate and decapsulate those keys.
     * <p>
     * The Java runtime determines and provides the {@link Provider} used for local AES data key generation according to
     * its configuration. Google Cloud KMS encrypts and decrypts the generated data keys using the key identified by the
     * provided {@link CloudKmsKeyEncapsulationOptions}.
     * <p>
     * The Google Cloud KMS client is used for communication with Google Cloud KMS. Google Cloud credentials are
     * resolved by the client using the Google Cloud client default credential resolution mechanism.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @throws IOException when Cloud KMS client fails
     * @return the new instance
     */
    public static AsyncKeyEncapsulationService create(CloudKmsKeyEncapsulationOptions defaultOptions)
            throws NoSuchAlgorithmException, IOException {
        Objects.requireNonNull(defaultOptions);
        return new CloudKeyEncapsulationService(defaultOptions, null);
    }

    /**
     * Returns a new {@link AsyncKeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Google Cloud KMS to encapsulate and decapsulate those keys.
     * <p>
     * The supplied {@link Provider} is used for local AES data key generation. Google Cloud KMS encrypts and decrypts
     * the generated data keys using the key identified by the provided {@link CloudKmsKeyEncapsulationOptions}.
     * <p>
     * The Google Cloud KMS client is used for communication with Google Cloud KMS. Google Cloud credentials are
     * resolved by the client using the Google Cloud client default credential resolution mechanism.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @param provider the {@link Provider} to use for cryptographic operations, must not be {@literal null}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @throws IOException when Cloud KMS client fails
     * @return the new instance
     */
    public static AsyncKeyEncapsulationService create(CloudKmsKeyEncapsulationOptions defaultOptions, Provider provider)
            throws NoSuchAlgorithmException, IOException {
        Objects.requireNonNull(defaultOptions);
        Objects.requireNonNull(provider);
        return new CloudKeyEncapsulationService(defaultOptions, provider);
    }
}
