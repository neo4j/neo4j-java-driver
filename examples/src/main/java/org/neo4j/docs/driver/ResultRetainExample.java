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

import static org.neo4j.driver.Values.parameters;

public class ResultRetainExample extends BaseApplication {
    public ResultRetainExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::result-retain[]
    @SuppressWarnings("UnusedReturnValue")
    public int addEmployees(final String companyName) {
        try (var session = driver.session()) {
            var persons = session.executeRead(tx -> tx.run("MATCH (a:Person) RETURN a.name AS name").list());
            return persons.stream().mapToInt(person -> session.executeWrite(tx -> {
                var result = tx.run(
                        "MATCH (emp:Person {name: $person_name}) " + "MERGE (com:Company {name: $company_name}) "
                                + "MERGE (emp)-[:WORKS_FOR]->(com)",
                        parameters("person_name", person.get("name").asString(), "company_name", companyName));
                result.consume();
                return 1;
            })).sum();
        }
    }
    // end::result-retain[]

}
