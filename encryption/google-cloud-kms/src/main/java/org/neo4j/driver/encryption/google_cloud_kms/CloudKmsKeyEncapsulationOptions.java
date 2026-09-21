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
package org.neo4j.driver.encryption.google_cloud_kms;

import com.google.cloud.kms.v1.CryptoKeyName;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.util.Preview;

/**
 * Options used by a {@link CloudKeyEncapsulationService}.
 *
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface CloudKmsKeyEncapsulationOptions extends KeyEncapsulationOptions
        permits CloudKmsKeyEncapsulationOptionsImpl {
    /**
     * Creates a new instance.
     * @param project the project
     * @param location the location
     * @param keyRing the key ring
     * @param cryptoKey the crypto key
     * @return the new instance
     */
    static CloudKmsKeyEncapsulationOptions of(String project, String location, String keyRing, String cryptoKey) {
        return new CloudKmsKeyEncapsulationOptionsImpl(project, location, keyRing, cryptoKey);
    }

    /**
     * Returns the key name.
     * @return the key name
     */
    CryptoKeyName keyName();
}
