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
package org.neo4j.driver;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.SecurityRetryableException;

/**
 * A manager of {@link AuthToken} instances used by the driver.
 * <p>
 * The manager must manage tokens for the same identity. Therefore, it is not intended for a change of identity.
 * <p>
 * Implementations should supply the same token unless it needs to be updated since a change of token might result in
 * extra processing by the driver.
 * <p>
 * Driver initializes new connections with a token supplied by the manager. If token changes, driver action depends on
 * connection's Bolt protocol version:
 * <ul>
 *     <li>Bolt 5.1 or above - {@code LOGOFF} and {@code LOGON} messages are dispatched to update the token on next interaction</li>
 *     <li>Bolt 5.0 or below - connection is closed an a new one is initialized with the new token</li>
 * </ul>
 * <p>
 * All implementations of this interface must be thread-safe and non-blocking for caller threads. For instance, IO operations must not
 * be done on the calling thread.
 * @since 5.8
 */
public interface AuthTokenManager {
    /**
     * Returns a {@link CompletionStage} for a valid {@link AuthToken}.
     * <p>
     * Driver invokes this method often to check if token has changed.
     * <p>
     * Failures will surface via the driver API, like {@link Session#beginTransaction()} method and others.
     * @return a stage for a valid token, must not be {@code null} or complete with {@code null}
     * @see AuthTokenManagerExecutionException
     */
    CompletionStage<AuthToken> getToken();

    /**
     * Handles {@link SecurityException} that is created based on the server's security error response by determining if
     * the given error may be resolved upon next {@link AuthTokenManager#getToken()} invokation.
     * <p>
     * If this method returns {@code true}, the driver wraps the original {@link SecurityException} in
     * {@link SecurityRetryableException}. The managed transaction API (like
     * {@link Session#executeRead(TransactionCallback)}, etc.) automatically retries its unit of work if no other
     * condition is violated, while the other query execution APIs surface this error for external handling.
     * <p>
     * If this method returns {@code false}, the original error remains unchanged.
     * <p>
     * This method must not throw exceptions.
     *
     * @param authToken the current token
     * @param exception the security exception
     * @return {@code true} if the exception should be marked as retryable or {@code false} if it should remain unchanged
     * @since 5.12
     */
    boolean handleSecurityException(AuthToken authToken, SecurityException exception);
}
