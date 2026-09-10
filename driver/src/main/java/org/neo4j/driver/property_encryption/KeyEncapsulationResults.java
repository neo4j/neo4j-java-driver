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
package org.neo4j.driver.property_encryption;

import java.util.Map;
import javax.crypto.SecretKey;
import org.neo4j.driver.internal.property_encryption.InternalKeyEncapsulationResult;
import org.neo4j.driver.util.Preview;

/**
 * A factory for {@link KeyEncapsulationResult}.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public final class KeyEncapsulationResults {
    private KeyEncapsulationResults() {}

    /**
     * Creates a new encapsulation result.
     *
     * @param encapsulation the encapsulation bytes, must not be {@literal null}
     * @param metadata      the encapsulation metadata, must not be {@literal null}
     * @param key           the new key, must not be {@literal null}
     * @return the new encapsulation result
     */
    public static KeyEncapsulationResult create(byte[] encapsulation, Map<String, String> metadata, SecretKey key) {
        return new InternalKeyEncapsulationResult(encapsulation, metadata, key);
    }
}
