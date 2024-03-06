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
package org.neo4j.driver;

/**
 * Callback that executes operations in a given {@link TransactionContext}.
 *
 * @param <T> the return type of this work.
 */
public interface TransactionCallback<T> {
    /**
     * Executes all given operations in the same transaction context.
     *
     * @param context the transaction context to use.
     * @return result object or {@code null} if none.
     */
    T execute(TransactionContext context);
}