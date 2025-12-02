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
package org.neo4j.driver.property_encryption.async;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.util.Preview;

/**
 * An asynchronous Neo4j Property encryption.
 * @see org.neo4j.driver.property_encryption.PropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface AsyncPropertyEncryption extends BasePropertyEncryption<AsyncEncapsulatedKeyManager> {
    /**
     * Handles the provided {@link PropertyEncryptRequest}.
     * @param encryptRequest the request
     * @return the encrypted bytes
     */
    CompletionStage<byte[]> encryptToBytesAsync(PropertyEncryptRequest encryptRequest);

    /**
     * Handles the provided {@link PropertyDecryptRequest}.
     * @param decryptRequest the request
     * @return the decrypted value
     */
    CompletionStage<Value> decryptAsync(PropertyDecryptRequest decryptRequest);
}
