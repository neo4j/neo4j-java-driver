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
package org.neo4j.driver.encryption.aws_kms;

import java.util.Map;
import java.util.Objects;

final class AwsKeyEncapsulationOptionsImpl implements AwsKeyEncapsulationOptions {
    static final String KMS_KEY_ID = "aws_key_id";

    static AwsKeyEncapsulationOptionsImpl of(Map<String, String> metadata) {
        var keyId = metadata.get(KMS_KEY_ID);
        return new AwsKeyEncapsulationOptionsImpl(keyId);
    }

    private final String keyId;

    AwsKeyEncapsulationOptionsImpl(String keyId) {
        this.keyId = Objects.requireNonNull(keyId);
    }

    @Override
    public String keyId() {
        return keyId;
    }

    @Override
    public Map<String, String> toMap() {
        return Map.of(KMS_KEY_ID, keyId);
    }
}
