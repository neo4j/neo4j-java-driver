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
package org.neo4j.driver.integration;

import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryption;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;

public class PropertyEncryptionIT extends AbstractPropertyEncryptionIT<PropertyEncryption> {
    @Override
    protected Class<PropertyEncryption> propertyEncryptionType() {
        return PropertyEncryption.class;
    }

    @Override
    protected byte[] encryptToBytes(PropertyEncryptionRequest request) {
        return propertyEncryption.encryptToBytes(request);
    }

    @Override
    protected Value decrypt(PropertyDecryptionRequest request) {
        return propertyEncryption.decrypt(request);
    }

    @Override
    protected void createKey() {
        propertyEncryption.keyManager().create();
    }
}
