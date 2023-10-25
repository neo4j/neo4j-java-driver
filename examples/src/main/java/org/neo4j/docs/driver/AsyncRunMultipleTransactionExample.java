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
package org.neo4j.docs.driver;

import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.neo4j.driver.Values.parameters;

public class AsyncRunMultipleTransactionExample extends BaseApplication {
    public AsyncRunMultipleTransactionExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::async-multiple-tx[]
    public CompletionStage<Integer> addEmployees(final String companyName) {
        var session = driver.session(AsyncSession.class);
        return session.executeReadAsync(AsyncRunMultipleTransactionExample::matchPersonNodes)
                .thenCompose(personNames -> session.executeWriteAsync(tx -> createNodes(tx, companyName, personNames)));
    }

    private static CompletionStage<List<String>> matchPersonNodes(AsyncTransactionContext tx) {
        return tx.runAsync("MATCH (a:Person) RETURN a.name AS name")
                .thenCompose(cursor -> cursor.listAsync(record -> record.get("name").asString()));
    }

    private static CompletionStage<Integer> createNodes(AsyncTransactionContext tx, String companyName, List<String> personNames) {
        return personNames.stream()
                .map(personName -> createNode(tx, companyName, personName))
                .reduce(CompletableFuture.completedFuture(0), (stage1, stage2) -> stage1.thenCombine(stage2, Integer::sum));
    }

    private static CompletionStage<Integer> createNode(AsyncTransactionContext tx, String companyName, String personName) {
        return tx.runAsync("MATCH (emp:Person {name: $person_name}) MERGE (com:Company {name: $company_name}) MERGE (emp)-[:WORKS_FOR]->(com)",
                        parameters("person_name", personName, "company_name", companyName))
                .thenCompose(ResultCursor::consumeAsync)
                .thenApply(ResultSummary::counters)
                .thenApply(SummaryCounters::nodesCreated);
    }
    // end::async-multiple-tx[]
}
