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

import org.neo4j.driver.Query;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class AsyncResultConsumeExample extends BaseApplication {
    public AsyncResultConsumeExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::async-result-consume[]
    public CompletionStage<List<String>> getPeople() {
        var query = new Query("MATCH (a:Person) RETURN a.name ORDER BY a.name");
        var session = driver.asyncSession();
        return session.executeReadAsync(tx -> tx.runAsync(query)
                .thenCompose(cursor -> cursor.listAsync(record -> record.get(0).asString())));
    }
    // end::async-result-consume[]
}
