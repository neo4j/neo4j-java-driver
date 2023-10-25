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

import java.io.Serial;

/**
 * This is the base class for Neo4j exceptions.
 *
 * @since 1.0
 */
public class Neo4jException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -80579062276712566L;

    /**
     * The code value.
     */
    private final String code;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public Neo4jException(String message) {
        this("N/A", message);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param cause the cause
     */
    public Neo4jException(String message, Throwable cause) {
        this("N/A", message, cause);
    }

    /**
     * Creates a new instance.
     * @param code the code
     * @param message the message
     */
    public Neo4jException(String code, String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the code
     * @param message the message
     * @param cause the cause
     */
    public Neo4jException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /**
     * Access the status code for this exception. The Neo4j manual can
     * provide further details on the available codes and their meanings.
     *
     * @return textual code, such as "Neo.ClientError.Procedure.ProcedureNotFound"
     */
    public String code() {
        return code;
    }
}
