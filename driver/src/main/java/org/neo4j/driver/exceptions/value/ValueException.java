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
package org.neo4j.driver.exceptions.value;

import java.io.Serial;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.util.Preview;

/**
 * A <em>ValueException</em> indicates that the client has carried out an operation on values incorrectly.
 * @since 1.0
 */
public class ValueException extends ClientException {
    @Serial
    private static final long serialVersionUID = -1269336313727174998L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public ValueException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param cause the cause
     * @since 5.28.5
     */
    @Preview(name = "Object mapping")
    public ValueException(String message, Throwable cause) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }
}
