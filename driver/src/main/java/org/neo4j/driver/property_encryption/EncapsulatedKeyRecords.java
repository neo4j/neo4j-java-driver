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
import org.neo4j.driver.internal.InternalEncapsulatedKeyRecord;
import org.neo4j.driver.util.Preview;

/**
 * A factory for {@link EncapsulatedKeyRecord}.
 * @since 6.3.0
 */
@Preview(name = "Property Encryption")
public final class EncapsulatedKeyRecords {
    private EncapsulatedKeyRecords() {}

    /**
     * Returns a new instance of {@link EncapsulatedKeyRecord}.
     * @param id the key id, must not be {@literal null}
     * @param alias the key alias, may be {@literal null}
     * @param encapsulation the key encapsulation, must not be {@literal null}
     * @param metadata the key metadata, must not be {@literal null}
     * @return the new instance of encapsulated key record
     */
    public static EncapsulatedKeyRecord create(
            String id, String alias, byte[] encapsulation, Map<String, String> metadata) {
        return new InternalEncapsulatedKeyRecord(id, alias, encapsulation, metadata);
    }
}
