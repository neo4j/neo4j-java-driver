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

import java.security.Provider;
import java.security.SecureRandom;
import org.neo4j.driver.util.Preview;

/**
 * A cryptographic context used for cryptographic purposes.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface CryptoContext {
    /**
     * Returns the {@link Provider} that must be used for cryptographic purposes
     * @return the provider
     */
    Provider provider();

    /**
     * Returns the {@link SecureRandom} that must be used for IV generation.
     * @return the secure random generator
     */
    SecureRandom ivSecureRandom();
}
