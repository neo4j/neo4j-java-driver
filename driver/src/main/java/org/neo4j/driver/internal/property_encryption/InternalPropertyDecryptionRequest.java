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
package org.neo4j.driver.internal.property_encryption;

import java.util.Objects;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;

public final class InternalPropertyDecryptionRequest extends AbstractPropertyRequest
        implements PropertyDecryptionRequest.ValueStep,
                PropertyDecryptionRequest.AADStep,
                PropertyDecryptionRequest.BuildStep,
                PropertyDecryptionRequest {
    byte[] value;
    String encryptionKeyId;
    String encryptionKeyAlias;
    KeyEncapsulationOptions encryptionKeyEncapsulationOptions;
    Value aad;

    @Override
    public AADStep fromValue(byte[] value) {
        this.value = Objects.requireNonNull(value);
        return this;
    }

    @Override
    public BuildStep withAAD(Value aad) {
        validate(aad);
        this.aad = aad;
        return this;
    }

    @Override
    public BuildStep withPersistedAAD() {
        this.aad = null;
        return this;
    }

    @Override
    public PropertyDecryptionRequest build() {
        return this;
    }
}
