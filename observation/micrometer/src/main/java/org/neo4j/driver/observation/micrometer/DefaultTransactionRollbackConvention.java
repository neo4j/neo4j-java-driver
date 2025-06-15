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
public class DefaultTransactionRollbackConvention implements TransactionRollbackConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.TransactionRollbackLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    static final DefaultTransactionRollbackConvention INSTANCE = new DefaultTransactionRollbackConvention();

    public DefaultTransactionRollbackConvention() {}

    @Override
    public String getName() {
        return "neo4j.db.client.transaction.rollback.duration";
    }

    @Override
    public String getContextualName(TransactionRollbackContext context) {
        return "transaction rollback";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(TransactionRollbackContext context) {
        return KeyValues.of(DB_SYSTEM_NAME, transactionType(context));
    }

    private KeyValue transactionType(TransactionRollbackContext context) {
        return Neo4jDriverDocumentation.TransactionRollbackLowCardinalityKeyNames.TRANSACTION_TYPE.withValue(
                context.transactionType().getSimpleName());
    }
}
