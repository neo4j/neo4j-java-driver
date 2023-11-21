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

import java.util.ArrayList;
import java.util.List;

public class ResultConsumeExample extends BaseApplication {
    public ResultConsumeExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::result-consume[]
    public List<String> getPeople() {
        try (var session = driver.session()) {
            return session.executeRead(tx -> {
                List<String> names = new ArrayList<>();
                var result = tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name");
                while (result.hasNext()) {
                    names.add(result.next().get(0).asString());
                }
                return names;
            });
        }
    }
    // end::result-consume[]
}
