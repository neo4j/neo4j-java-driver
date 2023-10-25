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

// tag::pass-bookmarks-import[]

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.TransactionContext;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;
// end::pass-bookmarks-import[]

public class PassBookmarkExample extends BaseApplication {

    public PassBookmarkExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::pass-bookmarks[]
    // Create a company node
    private void addCompany(TransactionContext tx, String name) {
        tx.run("CREATE (:Company {name: $name})", parameters("name", name));
    }

    // Create a person node
    private void addPerson(TransactionContext tx, String name) {
        tx.run("CREATE (:Person {name: $name})", parameters("name", name));
    }

    // Create an employment relationship to a pre-existing company node.
    // This relies on the person first having been created.
    private void employ(TransactionContext tx, String person, String company) {
        tx.run("MATCH (person:Person {name: $person_name}) MATCH (company:Company {name: $company_name}) CREATE (person)-[:WORKS_FOR]->(company)",
                parameters("person_name", person, "company_name", company));
    }

    // Create a friendship between two people.
    @SuppressWarnings("SameParameterValue")
    private void makeFriends(TransactionContext tx, String person1, String person2) {
        tx.run("MATCH (a:Person {name: $person_1}) MATCH (b:Person {name: $person_2}) MERGE (a)-[:KNOWS]->(b)",
                parameters("person_1", person1, "person_2", person2));
    }

    // Match and display all friendships.
    private void printFriends(TransactionContext tx) {
        var result = tx.run("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name");
        while (result.hasNext()) {
            var record = result.next();
            System.out.printf("%s knows %s%n", record.get("a.name").asString(), record.get("b.name").toString());
        }
    }

    public void addEmployAndMakeFriends() {
        // To collect the session bookmarks
        List<Bookmark> savedBookmarks;

        // Create the first person and employment relationship.
        try (var session1 =
                driver.session(builder().withDefaultAccessMode(AccessMode.WRITE).build())) {
            session1.executeWriteWithoutResult(tx -> addCompany(tx, "Wayne Enterprises"));
            session1.executeWriteWithoutResult(tx -> addPerson(tx, "Alice"));
            session1.executeWriteWithoutResult(tx -> employ(tx, "Alice", "Wayne Enterprises"));

            savedBookmarks = new ArrayList<>(session1.lastBookmarks());
        }

        // Create the second person and employment relationship.
        try (var session2 =
                driver.session(builder().withDefaultAccessMode(AccessMode.WRITE).build())) {
            session2.executeWriteWithoutResult(tx -> addCompany(tx, "LexCorp"));
            session2.executeWriteWithoutResult(tx -> addPerson(tx, "Bob"));
            session2.executeWriteWithoutResult(tx -> employ(tx, "Bob", "LexCorp"));

            savedBookmarks.addAll(session2.lastBookmarks());
        }

        // Create a friendship between the two people created above.
        try (var session3 = driver.session(builder()
                .withDefaultAccessMode(AccessMode.WRITE)
                .withBookmarks(savedBookmarks)
                .build())) {
            session3.executeWriteWithoutResult(tx -> makeFriends(tx, "Alice", "Bob"));

            session3.executeWriteWithoutResult(this::printFriends);
        }
    }
    // end::pass-bookmarks[]
}
