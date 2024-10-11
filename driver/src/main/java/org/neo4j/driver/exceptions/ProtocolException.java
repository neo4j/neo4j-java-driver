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
import org.neo4j.driver.internal.GqlStatusError;

/**
 * A signal that the contract for client-server communication has broken down.
 * The user should contact support and cannot resolve this his or herself.
 */
public class ProtocolException extends Neo4jException {
    @Serial
    private static final long serialVersionUID = -6806924452045883275L;

    private static final String CODE = "Protocol violation: ";

    /**
     * Creates a new instance.
     * @param message the message
     */
    public ProtocolException(String message) {
        this(CODE + message, null);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param e the throwable
     */
    public ProtocolException(String message, Throwable e) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                CODE + message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                e);
    }
}
