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
package org.neo4j.driver.property_encryption.reactive;

import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptRequest;
import org.neo4j.driver.util.Preview;

/**
 * A reactive Neo4j Property encryption.
 * @see org.neo4j.driver.property_encryption.PropertyEncryption
 * @see org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface ReactivePropertyEncryption extends BasePropertyEncryption<ReactiveEncapsulatedKeyManager> {
    /**
     * Handles the provided {@link PropertyEncryptRequest}.
     * @param encryptRequest the request
     * @return the encrypted bytes
     */
    Publisher<byte[]> encryptToBytes(PropertyEncryptRequest encryptRequest);

    /**
     * Handles the provided {@link PropertyDecryptRequest}.
     * @param decryptRequest the request
     * @return the decrypted value
     */
    Publisher<Value> decrypt(PropertyDecryptRequest decryptRequest);
}
