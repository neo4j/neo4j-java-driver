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

// tag::custom-auth-import[]
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.util.Map;
// end::custom-auth-import[]

public class CustomAuthExample implements AutoCloseable {
    private final Driver driver;

    // tag::custom-auth[]
    public CustomAuthExample(
            String uri,
            String principal,
            String credentials,
            String realm,
            String scheme,
            Map<String, Object> parameters) {
        driver = GraphDatabase.driver(uri, AuthTokens.custom(principal, credentials, realm, scheme, parameters));
    }
    // end::custom-auth[]

    public boolean canConnect() {
        var result = driver.session().run("RETURN 1");
        return result.single().get(0).asInt() == 1;
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }
}
