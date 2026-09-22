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
package org.neo4j.driver.encryption;

import java.util.Map;
import org.neo4j.driver.util.Preview;

/**
 * Options used by a {@link BaseKeyEncapsulationService} to encapsulate a key.
 * <p>
 * Implementations of {@link BaseKeyEncapsulationService} are expected to define a dedicated subtype of this interface
 * containing the options required by the particular key encapsulation mechanism.
 *
 * @see BaseKeyEncapsulationService
 * @see KeyEncapsulationResult
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public interface KeyEncapsulationOptions {
    /**
     * Returns the options as a {@link Map}.
     *
     * @return the options as a map
     */
    Map<String, String> toMap();
}
