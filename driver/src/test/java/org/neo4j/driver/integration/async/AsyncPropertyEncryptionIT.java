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
package org.neo4j.driver.integration.async;

import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.integration.AbstractPropertyEncryptionIT;
import org.neo4j.driver.internal.util.Futures;

class AsyncPropertyEncryptionIT extends AbstractPropertyEncryptionIT<AsyncPropertyEncryption> {
    @Override
    protected Class<AsyncPropertyEncryption> propertyEncryptionType() {
        return AsyncPropertyEncryption.class;
    }

    @Override
    protected byte[] encryptToBytes(PropertyEncryptionRequest request) {
        return Futures.blockingGet(propertyEncryption.encryptToBytesAsync(request));
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return Futures.blockingGet(propertyEncryption.decryptAsync(request));
    }

    @Override
    protected void createKey() {
        Futures.blockingGet(propertyEncryption.keyManager().createAsync());
    }
}
