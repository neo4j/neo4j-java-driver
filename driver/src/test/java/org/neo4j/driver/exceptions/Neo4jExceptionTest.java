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
package org.neo4j.driver.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;

class Neo4jExceptionTest {

    @Test
    void shouldInit() {
        var gqlStatus = "status";
        var description = "description";
        var code = "code";
        var message = "message";
        var map = new HashMap<String, Value>();
        var throwable = new Neo4jException(gqlStatus, description, code, message, map, null);

        var exception = new Neo4jException(gqlStatus, description, code, message, map, throwable);

        assertEquals(gqlStatus, exception.gqlStatus());
        assertEquals(description, exception.statusDescription());
        assertEquals(code, exception.code());
        assertEquals(message, exception.getMessage());
        assertEquals(map, exception.diagnosticRecord());
        assertEquals(throwable, exception.gqlCause().orElse(null));
    }
}
