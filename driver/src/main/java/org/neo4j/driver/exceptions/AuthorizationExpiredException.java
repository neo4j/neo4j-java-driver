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
 * The authorization info maintained on the server has expired. The client should reconnect.
 * <p>
 * Error code: Neo.ClientError.Security.AuthorizationExpired
 */
public class AuthorizationExpiredException extends SecurityException implements RetryableException {
    @Serial
    private static final long serialVersionUID = 5688002170978405558L;

    /**
     * The description for {@link AuthorizationExpiredException}.
     */
    public static final String DESCRIPTION =
            "Authorization information kept on the server has expired, this connection is no longer valid.";

    /**
     * Creates a new instance.
     * @param code the code
     * @param message the message
     */
    public AuthorizationExpiredException(String code, String message) {
        super(code, message);
    }
}
