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

import java.util.Map;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import org.neo4j.driver.util.Preview;

/**
 * A service responsible for encapsulating and decapsulating keys.
 * <p>
 * Implementations MUST be non-blocking. In particular, implementations MUST NOT perform blocking operations on
 * the calling thread.
 * <p>
 * Implementations MUST supply 256-bit AES keys only.
 *
 * @see KeyEncapsulationOptions
 * @see KeyEncapsulationResult
 * @see KeyEncapsulationServices
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface KeyEncapsulationService {
    /**
     * Creates a new key, encapsulates it and returns the result.
     *
     * @param options the encapsulation options
     * @return a {@link CompletionStage} that completes with the encapsulation result
     */
    CompletionStage<KeyEncapsulationResult> encapsulate(KeyEncapsulationOptions options);

    /**
     * Decapsulates encapsulated bytes.
     *
     * @param encapsulation the encapsulated bytes, must not be {@code null}
     * @param metadata      the key metadata, must not be {@code null}
     * @return a {@link CompletionStage} that completes with the decapsulated key
     */
    CompletionStage<SecretKey> decapsulate(byte[] encapsulation, Map<String, String> metadata);
}
