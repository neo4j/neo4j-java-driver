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
package org.neo4j.driver.internal;

import java.util.Map;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.Value;

final class DelegatingTransactionContext implements TransactionContext {
    private final Transaction delegate;

    public DelegatingTransactionContext(Transaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Result run(String query, Value parameters) {
        return delegate.run(query, parameters);
    }

    @Override
    public Result run(String query, Map<String, Object> parameters) {
        return delegate.run(query, parameters);
    }

    @Override
    public Result run(String query, Record parameters) {
        return delegate.run(query, parameters);
    }

    @Override
    public Result run(String query) {
        return delegate.run(query);
    }

    @Override
    public Result run(Query query) {
        return delegate.run(query);
    }
}
