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
package org.neo4j.driver.internal.encryption;

import java.security.ProviderException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationResult;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;

final class ValidatingAsyncKeyEncapsulationService implements AsyncKeyEncapsulationService {
    private static final String KEY_ALGORITHM = "AES";
    private static final int KEY_LENGTH = 32;
    private final AsyncKeyEncapsulationService delegate;

    ValidatingAsyncKeyEncapsulationService(AsyncKeyEncapsulationService delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public CompletionStage<KeyEncapsulationResult> encapsulateAsync(KeyEncapsulationOptions options) {
        return delegate.encapsulateAsync(options).thenApply(result -> {
            Objects.requireNonNull(result);
            Objects.requireNonNull(result.encapsulation());
            Objects.requireNonNull(result.metadata());
            validate(result.key());
            return result;
        });
    }

    @Override
    public CompletionStage<SecretKey> decapsulateAsync(byte[] encapsulation, Map<String, String> metadata) {
        return delegate.decapsulateAsync(encapsulation, metadata).thenApply(this::validate);
    }

    private SecretKey validate(SecretKey secretKey) {
        Objects.requireNonNull(secretKey);
        if (!secretKey.getAlgorithm().equals(KEY_ALGORITHM)) {
            throw new ProviderException("Invalid key algorithm: " + secretKey.getAlgorithm());
        }
        if (secretKey.getEncoded().length != KEY_LENGTH) {
            throw new ProviderException("Invalid key length: %d bytes" + secretKey.getEncoded().length);
        }
        return secretKey;
    }
}
