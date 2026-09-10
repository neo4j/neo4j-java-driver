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
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;

public final class InternalPropertyEncryptionRequest extends AbstractPropertyRequest
        implements PropertyEncryptionRequest.ProfileStep,
                PropertyEncryptionRequest.ValueStep,
                PropertyEncryptionRequest.EncryptionKeyReferenceStep,
                PropertyEncryptionRequest.AADStep,
                PropertyEncryptionRequest.BuildStep,
                PropertyEncryptionRequest {
    Value value;
    String encryptionKeyId;
    String encryptionKeyAlias;
    Value aad;
    String profileName;
    byte[] iv; // for testing purposes only

    @Override
    public EncryptionKeyReferenceStep usingProfile(String profileName) {
        this.profileName = Objects.requireNonNull(profileName);
        return this;
    }

    @Override
    public AADStep fromValue(Value value) {
        validate(value);
        this.value = Objects.requireNonNull(value);
        return this;
    }

    @Override
    public BuildStep usingKeyId(String keyId) {
        this.encryptionKeyId = Objects.requireNonNull(keyId);
        return this;
    }

    @Override
    public BuildStep usingKeyAlias(String keyAlias) {
        this.encryptionKeyAlias = Objects.requireNonNull(keyAlias);
        return this;
    }

    @Override
    public AADStep withAAD(Value aad) {
        if (value == null || TYPE_SYSTEM.NULL().isTypeOf(value)) {
            // Both null and NULL value disable AAD
            return this;
        }
        validateAad(aad);
        this.aad = aad;
        return this;
    }

    @Override
    public PropertyEncryptionRequest build() {
        return this;
    }
}
