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

import java.util.Optional;
import org.neo4j.driver.util.Preview;

/**
 * An encapsulated key.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface EncapsulatedKey {
    /**
     * Returns the key id.
     * @return the key id
     */
    String id();

    /**
     * Returns the key alias if assigned.
     * @return the key alias or {@link Optional#empty()} otherwise
     */
    Optional<String> alias();
}
