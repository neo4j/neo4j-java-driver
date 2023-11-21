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

import org.neo4j.driver.TransactionConfig;

import java.util.Map;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;

public class TransactionMetadataConfigExample extends BaseApplication {
    public TransactionMetadataConfigExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::transaction-metadata-config[]
    public void addPerson(final String name) {
        try (var session = driver.session()) {
            var txConfig = TransactionConfig.builder()
                    .withMetadata(Map.of("applicationId", value("123")))
                    .build();
            session.executeWriteWithoutResult(tx -> tx.run("CREATE (a:Person {name: $name})", parameters("name", name)).consume(), txConfig);
        }
    }
    // end::transaction-metadata-config[]
}
