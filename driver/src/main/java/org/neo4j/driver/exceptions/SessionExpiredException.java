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
 * A <em>SessionExpiredException</em> indicates that the session can no longer satisfy the criteria under which it was acquired, e.g. a server no longer accepts
 * write requests. A new session needs to be acquired from the driver and all actions taken on the expired session must be replayed.
 *
 * @since 1.1
 */
public class SessionExpiredException extends Neo4jException implements RetryableException {
    @Serial
    private static final long serialVersionUID = 843176371236755724L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public SessionExpiredException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param throwable the throwable
     */
    public SessionExpiredException(String message, Throwable throwable) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                throwable);
    }
}
