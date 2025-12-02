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

import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.util.Preview;

/**
 * Options used by a {@link AWSKeyEncapsulationService}.
 *
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface AwsKeyEncapsulationOptions extends KeyEncapsulationOptions
        permits AwsKeyEncapsulationOptionsImpl {
    /**
     * Creates a new instance.
     * @param keyId the key id
     * @return the new instance
     */
    static AwsKeyEncapsulationOptions of(String keyId) {
        return new AwsKeyEncapsulationOptionsImpl(keyId);
    }

    /**
     * Returns the key id.
     * @return the key id
     */
    String keyId();
}
