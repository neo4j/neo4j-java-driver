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

// tag::read-write-transaction-import[]

import org.neo4j.driver.TransactionContext;

import static org.neo4j.driver.Values.parameters;
// end::read-write-transaction-import[]

public class ReadWriteTransactionExample extends BaseApplication {
    public ReadWriteTransactionExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::read-write-transaction[]
    public long addPerson(final String name) {
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> tx.run("CREATE (a:Person {name: $name})", parameters("name", name)).consume());
            return session.executeRead(tx -> matchPersonNode(tx, name));
        }
    }

    private static long matchPersonNode(TransactionContext tx, String name) {
        var result = tx.run("MATCH (a:Person {name: $name}) RETURN id(a)", parameters("name", name));
        return result.single().get(0).asLong();
    }
    // end::read-write-transaction[]

}
