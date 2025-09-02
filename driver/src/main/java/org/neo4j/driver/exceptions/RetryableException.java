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
package org.neo4j.driver.exceptions;

import org.neo4j.driver.Session;

/**
 * A marker interface for retryable exceptions.
 * <p>
 * This indicates whether an operation that resulted in retryable exception might be resolved by retrying.
 * <p>
 * <b>Note</b> that some database requests executed via implicit transaction API (like, {@link Session#run(String)}),
 * might not be idempotent irrispective of this marker interface as its main focus is on the error type and not
 * individual database request. An example of such database request is
 * <a href="https://neo4j.com/docs/cypher-manual/current/subqueries/subqueries-in-transactions/">CALL {} IN TRANSACTIONS</a>.
 *
 * @since 5.0
 */
public interface RetryableException {}
