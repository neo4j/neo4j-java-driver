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
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.internal.GqlStatusError;

/**
 * The {@link org.neo4j.driver.AuthTokenManager} execution has lead to an unexpected result.
 * <p>
 * Possible causes include:
 * <ul>
 *     <li>{@link AuthTokenManager#getToken()} returned {@code null}</li>
 *     <li>{@link AuthTokenManager#getToken()} returned a {@link java.util.concurrent.CompletionStage} that completed with {@code null}</li>
 *     <li>{@link AuthTokenManager#getToken()} returned a {@link java.util.concurrent.CompletionStage} that completed with a token that was not created using {@link org.neo4j.driver.AuthTokens}</li>
 *     <li>{@link AuthTokenManager#getToken()} has thrown an exception</li>
 *     <li>{@link AuthTokenManager#getToken()} returned a {@link java.util.concurrent.CompletionStage} that completed exceptionally</li>
 * </ul>
 * @since 5.8
 */
public class AuthTokenManagerExecutionException extends ClientException {
    @Serial
    private static final long serialVersionUID = -5964665406806723214L;

    /**
     * Constructs a new instance.
     * @param message the message
     * @param cause the cause
     */
    public AuthTokenManagerExecutionException(String message, Throwable cause) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }
}
