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
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.util.Preview;

/**
 * Failed to communicate with the server due to security errors.
 * When this type of error happens, the security cause of the error should be fixed to ensure the safety of your data.
 * Restart of server/driver/cluster might be required to recover from this error.
 * @since 1.1
 */
public class SecurityException extends ClientException {
    @Serial
    private static final long serialVersionUID = -5964665406806523214L;

    /**
     * Creates a new instance.
     * @param code the code
     * @param message the message
     */
    // for testing only
    public SecurityException(String code, String message) {
        super(code, message);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param t the throwable
     */
    public SecurityException(String message, Throwable t) {
        this(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                t);
    }

    /**
     * Creates a new instance.
     * @param gqlStatus the GQLSTATUS as defined by the GQL standard
     * @param statusDescription the status description
     * @param code the code
     * @param message the message
     * @param diagnosticRecord the diagnostic record
     * @param cause the cause
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public SecurityException(
            String gqlStatus,
            String statusDescription,
            String code,
            String message,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {
        super(gqlStatus, statusDescription, code, message, diagnosticRecord, cause);
    }
}
