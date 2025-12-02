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
import org.neo4j.driver.util.Preview;

/**
 * @since 6.3.0
 */
@Preview(name = "Observability")
public class DefaultKeyEncapsulationServiceDecapsulateConvention
        implements KeyEncapsulationServiceDecapsulateConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.KeyEncapsulationServiceDecapsulateLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    static final DefaultKeyEncapsulationServiceDecapsulateConvention INSTANCE =
            new DefaultKeyEncapsulationServiceDecapsulateConvention();

    public DefaultKeyEncapsulationServiceDecapsulateConvention() {}

    @Override
    public String getName() {
        return "neo4j.db.client.property.encryption.key.decapsulate";
    }

    @Override
    public String getContextualName(KeyEncapsulationServiceDecapsulateContext context) {
        return "decapsulate key using service";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(KeyEncapsulationServiceDecapsulateContext context) {
        return KeyValues.of(DB_SYSTEM_NAME);
    }
}
