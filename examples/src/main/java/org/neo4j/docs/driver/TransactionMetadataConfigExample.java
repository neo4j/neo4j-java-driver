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
import static org.neo4j.driver.Values.value;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

public class TransactionMetadataConfigExample extends BaseApplication {
    public TransactionMetadataConfigExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::transaction-metadata-config[]
    public void addPerson(final String name) {
        try (Session session = driver.session()) {
            Map<String, Object> transactionMetadata = new HashMap<>();
            transactionMetadata.put("applicationId", value("123"));

            TransactionConfig txConfig = TransactionConfig.builder()
                    .withMetadata(transactionMetadata)
                    .build();
            session.writeTransaction(
                    tx -> {
                        tx.run("CREATE (a:Person {name: $name})", parameters("name", name))
                                .consume();
                        return 1;
                    },
                    txConfig);
        }
    }
    // end::transaction-metadata-config[]
}
