/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import java.time.Duration;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

public class TransactionTimeoutConfigExample extends BaseApplication {
    public TransactionTimeoutConfigExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    @SuppressWarnings("deprecation")
    // tag::transaction-timeout-config[]
    public void addPerson(final String name) {
        try (Session session = driver.session()) {
            TransactionConfig txConfig = TransactionConfig.builder()
                    .withTimeout(Duration.ofSeconds(5))
                    .build();
            session.writeTransaction(
                    tx -> {
                        tx.run("CREATE (a:Person {name: $name})", parameters("name", name));
                        return 1;
                    },
                    txConfig);
        }
    }
    // end::transaction-timeout-config[]
}
