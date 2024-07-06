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
 * An error has happened while getting routing table with a remote server.
 * While this error is not fatal and we might be able to recover if we continue trying on another server.
 * If we fail to get a valid routing table from all routing servers known to this driver,
 * then we will end up with a fatal error {@link ServiceUnavailableException}.
 * <p>
 * If you see this error in your logs, it is safe to ignore if your cluster is temporarily changing structure during that time.
 */
public class DiscoveryException extends Neo4jException {
    @Serial
    private static final long serialVersionUID = 6711564351333659090L;

    /**
     * Creates a new instance.
     * @param message the message
     * @param cause the cause
     */
    public DiscoveryException(String message, Throwable cause) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }
}
