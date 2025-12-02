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

import org.neo4j.driver.util.Preview;

/**
 * A profile for Neo4j Property Encryption.
 * <p>
 * Each profile instance represents a specific encryption configuration that MUST have a unique name, which is also
 * used for cross-driver interoperability.
 * <p>
 * While there may be several profile types in the future, only {@link EnvelopePropertyEncryptionProfile} is supported
 * for now.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public sealed interface PropertyEncryptionProfile permits EnvelopePropertyEncryptionProfile {
    /**
     * Returns the unique profile name.
     * @return the profile name
     */
    String name();
}
