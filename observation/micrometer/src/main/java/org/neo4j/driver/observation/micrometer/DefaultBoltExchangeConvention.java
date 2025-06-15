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
import java.util.List;

public class DefaultBoltExchangeConvention implements BoltExchangeConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.BoltExchangeLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    private static final KeyValue NETWORK_PROTOCOL_NAME =
            Neo4jDriverDocumentation.BoltExchangeLowCardinalityKeyNames.NETWORK_PROTOCOL_NAME.withValue("bolt");

    static final DefaultBoltExchangeConvention INSTANCE = new DefaultBoltExchangeConvention();

    public DefaultBoltExchangeConvention() {}

    @Override
    public String getName() {
        return "neo4j.bolt.client.exchange.duration";
    }

    @Override
    public String getContextualName(BoltExchangeContext context) {
        return "bolt exchange";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(BoltExchangeContext context) {
        return KeyValues.of(
                DB_SYSTEM_NAME,
                NETWORK_PROTOCOL_NAME,
                boltVersion(context),
                serverAddress(context),
                serverPort(context));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(BoltExchangeContext context) {
        return KeyValues.of(messages(context));
    }

    private KeyValue boltVersion(BoltExchangeContext context) {
        return Neo4jDriverDocumentation.BoltExchangeLowCardinalityKeyNames.NETWORK_PROTOCOL_VERSION.withValue(
                context.boltVersion());
    }

    private KeyValue serverAddress(BoltExchangeContext context) {
        return Neo4jDriverDocumentation.BoltExchangeLowCardinalityKeyNames.SERVER_ADDRESS.withValue(context.host());
    }

    private KeyValue serverPort(BoltExchangeContext context) {
        return Neo4jDriverDocumentation.BoltExchangeLowCardinalityKeyNames.SERVER_PORT.withValue(
                String.valueOf(context.port()));
    }

    private KeyValue messages(BoltExchangeContext context) {
        return Neo4jDriverDocumentation.BoltExchangeHighCardinalityKeyNames.MESSAGES.withValue(
                context.messageNames().map(List::toString).orElse(KeyValue.NONE_VALUE));
    }
}
