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
 * @since 6.0.0
 */
@Preview(name = "Observability")
public class DefaultPooledConnectionInUseConvention implements PooledConnectionInUseConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.PooledConnectionInUseLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    static final DefaultPooledConnectionInUseConvention INSTANCE = new DefaultPooledConnectionInUseConvention();

    public DefaultPooledConnectionInUseConvention() {}

    @Override
    public String getName() {
        return "db.client.connection.use.time";
    }

    @Override
    public String getContextualName(PooledConnectionInUseContext context) {
        return "connection use";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(PooledConnectionInUseContext context) {
        return KeyValues.of(DB_SYSTEM_NAME, poolName(context));
    }

    private KeyValue poolName(PooledConnectionInUseContext context) {
        return Neo4jDriverDocumentation.PooledConnectionInUseLowCardinalityKeyNames.POOL_NAME.withValue(context.id());
    }
}
