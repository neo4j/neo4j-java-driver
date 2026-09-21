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
package org.neo4j.driver.encryption.azure_keyvault;

import com.azure.security.keyvault.keys.cryptography.CryptographyAsyncClient;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Objects;
import org.neo4j.driver.encryption.KeyEncapsulationService;

/**
 * A factory for {@link KeyEncapsulationService} implementations using Azure Key Vault.
 *
 * @see KeyEncapsulationService
 * @since 6.3.0
 */
public final class AzureKeyEncapsulationServices {
    private AzureKeyEncapsulationServices() {}

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Azure Key Vault to encapsulate and decapsulate those keys.
     * <p>
     * The Java runtime determines and provides the {@link Provider} used for local AES data key generation according to
     * its configuration. Azure Key Vault encrypts and decrypts the generated data keys using the key identified by the
     * provided {@link AzureEncapsulationOptions}.
     * <p>
     * The service caches up to 10 {@link CryptographyAsyncClient} instances, keyed by Azure Key Vault key id. When the
     * cache reaches its capacity, the least recently used client is evicted. To configure a different cache size,
     * use {@link #create(AzureEncapsulationOptions, int)}.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @return the new instance
     */
    public static KeyEncapsulationService create(AzureEncapsulationOptions defaultOptions)
            throws NoSuchAlgorithmException {
        Objects.requireNonNull(defaultOptions);
        return new AzureKeyEncapsulationService(defaultOptions, null, 10);
    }

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Azure Key Vault to encapsulate and decapsulate those keys.
     * <p>
     * The Java runtime determines and provides the {@link Provider} used for local AES data key generation according to
     * its configuration. Azure Key Vault encrypts and decrypts the generated data keys using the key identified by the
     * provided {@link AzureEncapsulationOptions}.
     * <p>
     * The service caches {@link CryptographyAsyncClient} instances, keyed by Azure Key Vault key id. When the
     * cache reaches its capacity, the least recently used client is evicted. The maximum cache size is set by the
     * respective parameter.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @param clientCacheMaxSize the maximum cache size for {@link CryptographyAsyncClient} instances, must be greater
     *                           than {@literal 0}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @return the new instance
     */
    public static KeyEncapsulationService create(AzureEncapsulationOptions defaultOptions, int clientCacheMaxSize)
            throws NoSuchAlgorithmException {
        Objects.requireNonNull(defaultOptions);
        return new AzureKeyEncapsulationService(defaultOptions, null, clientCacheMaxSize);
    }

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Azure Key Vault to encapsulate and decapsulate those keys.
     * <p>
     * The supplied {@link Provider} is used for local AES data key generation. Azure Key Vault encrypts and decrypts
     * the generated data keys using the key identified by the provided {@link AzureEncapsulationOptions}.
     * <p>
     * Azure credentials are resolved using the Azure Identity library's default credential resolution mechanism.
     * <p>
     * The service caches up to 10 {@link CryptographyAsyncClient} instances, keyed by Azure Key Vault key id. When the
     * cache reaches its capacity, the least recently used client is evicted. To configure a different cache size,
     * use {@link #create(AzureEncapsulationOptions, int)}.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @param provider the {@link Provider} to use for cryptographic operations, must not be {@literal null}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @return the new instance
     */
    public static KeyEncapsulationService create(AzureEncapsulationOptions defaultOptions, Provider provider)
            throws NoSuchAlgorithmException {
        Objects.requireNonNull(defaultOptions);
        Objects.requireNonNull(provider);
        return new AzureKeyEncapsulationService(defaultOptions, provider, 10);
    }

    /**
     * Returns a new {@link KeyEncapsulationService} implementation that generates 256-bit AES data keys locally and
     * uses Azure Key Vault to encapsulate and decapsulate those keys.
     * <p>
     * The supplied {@link Provider} is used for local AES data key generation. Azure Key Vault encrypts and decrypts
     * the generated data keys using the key identified by the provided {@link AzureEncapsulationOptions}.
     * <p>
     * Azure credentials are resolved using the Azure Identity default credential resolution mechanism.
     * <p>
     * The service caches {@link CryptographyAsyncClient} instances, keyed by Azure Key Vault key id. When the
     * cache reaches its capacity, the least recently used client is evicted. The maximum cache size is set by the
     * respective parameter.
     *
     * @param defaultOptions the default options, must not be {@literal null}
     * @param provider the {@link Provider} to use for cryptographic operations, must not be {@literal null}
     * @param clientCacheMaxSize the maximum cache size for {@link CryptographyAsyncClient} instances, must be greater
     *                                 than {@literal 0}
     * @throws NoSuchAlgorithmException when no AES algorithm is found
     * @return the new instance
     */
    public static KeyEncapsulationService create(
            AzureEncapsulationOptions defaultOptions, Provider provider, int clientCacheMaxSize)
            throws NoSuchAlgorithmException {
        Objects.requireNonNull(defaultOptions);
        Objects.requireNonNull(provider);
        return new AzureKeyEncapsulationService(defaultOptions, provider, clientCacheMaxSize);
    }
}
