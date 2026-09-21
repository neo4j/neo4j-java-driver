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
package org.neo4j.driver.it.encryption.google_cloud_kms;

import java.util.Optional;
import java.util.stream.Stream;
import org.neo4j.driver.encryption.google_cloud_kms.CloudKmsKeyEncapsulationOptions;

final class OptionsLoader {
    static Optional<CloudKmsKeyEncapsulationOptions> fromEnv() {
        var project = System.getenv("TEST_GOOGLE_CLOUD_PROJECT");
        var location = System.getenv("TEST_GOOGLE_CLOUD_LOCATION");
        var keyRing = System.getenv("TEST_GOOGLE_CLOUD_KEY_RING");
        var cryptoKey = System.getenv("TEST_GOOGLE_CLOUD_CRYPTO_KEY");

        if (Stream.of(project, location, keyRing, cryptoKey).anyMatch(value -> value == null || value.isBlank())) {
            return Optional.empty();
        }

        return Optional.of(CloudKmsKeyEncapsulationOptions.of(project, location, keyRing, cryptoKey));
    }
}
