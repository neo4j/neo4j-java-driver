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
import java.util.Map;

final class CloudKmsKeyEncapsulationOptionsImpl implements CloudKmsKeyEncapsulationOptions {
    private static final String PROJECT = "google_project";
    private static final String LOCATION = "google_location";
    private static final String KEY_RING = "google_key_ring";
    private static final String CRYPTO_KEY = "google_crypto_key";

    static CloudKmsKeyEncapsulationOptionsImpl of(Map<String, String> metadata) {
        var project = metadata.get(PROJECT);
        var location = metadata.get(LOCATION);
        var keyRing = metadata.get(KEY_RING);
        var cryptoKey = metadata.get(CRYPTO_KEY);
        return new CloudKmsKeyEncapsulationOptionsImpl(project, location, keyRing, cryptoKey);
    }

    private final CryptoKeyName keyName;

    CloudKmsKeyEncapsulationOptionsImpl(String project, String location, String keyRing, String cryptoKey) {
        this.keyName = CryptoKeyName.of(project, location, keyRing, cryptoKey);
    }

    public CryptoKeyName keyName() {
        return keyName;
    }

    @Override
    public Map<String, String> toMap() {
        return Map.of(
                PROJECT,
                keyName.getProject(),
                LOCATION,
                keyName().getLocation(),
                KEY_RING,
                keyName().getKeyRing(),
                CRYPTO_KEY,
                keyName().getCryptoKey());
    }
}
