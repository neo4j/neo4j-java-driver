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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;

public class DefaultDeleteEncapsulatedKeyConvention implements DeleteEncapsulatedKeyConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.DeleteEncapsulatedKeyLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    static final DefaultDeleteEncapsulatedKeyConvention INSTANCE = new DefaultDeleteEncapsulatedKeyConvention();

    public DefaultDeleteEncapsulatedKeyConvention() {}

    @Override
    public String getName() {
        return "neo4j.db.client.property.encryption.delete.encapsulated.key.duration";
    }

    @Override
    public String getContextualName(DeleteEncapsulatedKeyContext context) {
        return "delete encapsulated key";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(DeleteEncapsulatedKeyContext context) {
        return KeyValues.of(DB_SYSTEM_NAME, encapsulatedKeyManagerType(context));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(DeleteEncapsulatedKeyContext context) {
        return KeyValues.of(id(context));
    }

    private KeyValue encapsulatedKeyManagerType(DeleteEncapsulatedKeyContext context) {
        return Neo4jDriverDocumentation.DeleteEncapsulatedKeyLowCardinalityKeyNames.ENCAPSULATED_KEY_MANAGER_TYPE
                .withValue(context.encapsulatedKeyManagerType().getSimpleName());
    }

    private KeyValue id(DeleteEncapsulatedKeyContext context) {
        return Neo4jDriverDocumentation.DeleteEncapsulatedKeyHighCardinalityKeyNames.KEY_ID.withValue(context.id());
    }
}
