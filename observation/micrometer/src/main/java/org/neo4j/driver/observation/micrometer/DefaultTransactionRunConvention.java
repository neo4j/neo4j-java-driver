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
public class DefaultTransactionRunConvention implements TransactionRunConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.TransactionRunLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    private final boolean alwaysAddQuery;
    private final boolean addParameters;

    public DefaultTransactionRunConvention(boolean alwaysAddQuery, boolean addParameters) {
        this.alwaysAddQuery = alwaysAddQuery;
        this.addParameters = addParameters;
    }

    @Override
    public String getName() {
        return "neo4j.db.client.transaction.run.duration";
    }

    @Override
    public String getContextualName(TransactionRunContext context) {
        return "transaction run";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(TransactionRunContext context) {
        return KeyValues.of(DB_SYSTEM_NAME, transactionType(context));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(TransactionRunContext context) {
        return KeyValuesUtil.queryAndParameters(
                Neo4jDriverDocumentation.TransactionRunHighCardinalityKeyNames.DB_QUERY_TEXT,
                context.query(),
                context.parameters(),
                Neo4jDriverDocumentation.TransactionRunHighCardinalityKeyNames.DB_QUERY_PARAMETER_FORMAT.asString(),
                alwaysAddQuery,
                addParameters);
    }

    private KeyValue transactionType(TransactionRunContext context) {
        return Neo4jDriverDocumentation.TransactionRunLowCardinalityKeyNames.TRANSACTION_TYPE.withValue(
                context.transactionType().getSimpleName());
    }
}
