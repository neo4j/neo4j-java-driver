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
package org.neo4j.driver.internal.adaptedbolt;

import java.io.Serial;
import java.util.Objects;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.driver.exceptions.Neo4jException;

final class BoltFailureExceptionWithNeo4jException extends BoltFailureException {
    @Serial
    private static final long serialVersionUID = 401078883000731869L;

    private final Neo4jException neo4jException;

    BoltFailureExceptionWithNeo4jException(BoltFailureException exception, Neo4jException neo4jException) {
        super(
                exception.code(),
                exception.getMessage(),
                exception.gqlStatus(),
                exception.statusDescription(),
                exception.diagnosticRecord(),
                exception.getCause());
        this.neo4jException = Objects.requireNonNull(neo4jException);
    }

    Neo4jException neo4jException() {
        return neo4jException;
    }
}
