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

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.internal.security.ExpirationBasedAuthTokenManager;

/**
 * Implementations of {@link AuthTokenManager}.
 *
 * @since 5.8
 */
public final class AuthTokenManagers {
    private AuthTokenManagers() {}

    /**
     * Returns an {@link AuthTokenManager} that manages basic {@link AuthToken} instances.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance, which would happen if
     * the server rejects the current token with {@link AuthenticationException} (see
     * {@link AuthTokenManager#handleSecurityException(AuthToken, SecurityException)}).
     * The provided supplier and its completion stages must be non-blocking as documented in the
     * {@link AuthTokenManager}.
     *
     * @param newTokenSupplier a new token stage supplier
     * @return a new token manager
     * @since 5.12
     */
    public static AuthTokenManager basic(Supplier<AuthToken> newTokenSupplier) {
        requireNonNull(newTokenSupplier, "newTokenSupplier must not be null");
        return basicAsync(() -> CompletableFuture.supplyAsync(newTokenSupplier));
    }

    /**
     * Returns an {@link AuthTokenManager} that manages basic {@link AuthToken} instances.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance, which would happen if
     * the server rejects the current token with {@link AuthenticationException} (see
     * {@link AuthTokenManager#handleSecurityException(AuthToken, SecurityException)}).
     * The provided supplier and its completion stages must be non-blocking as documented in the
     * {@link AuthTokenManager}.
     *
     * @param newTokenStageSupplier a new token stage supplier
     * @return a new token manager
     * @since 5.12
     */
    public static AuthTokenManager basicAsync(Supplier<CompletionStage<AuthToken>> newTokenStageSupplier) {
        requireNonNull(newTokenStageSupplier, "newTokenStageSupplier must not be null");
        return new ExpirationBasedAuthTokenManager(
                () -> newTokenStageSupplier.get().thenApply(authToken -> authToken.expiringAt(Long.MAX_VALUE)),
                Set.of(AuthenticationException.class),
                Clock.systemUTC());
    }

    /**
     * Returns an {@link AuthTokenManager} that manages bearer {@link AuthToken} instances with UTC expiration
     * timestamp.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance. This includes the
     * following conditions:
     * <ol>
     *     <li>token's UTC timestamp is expired</li>
     *     <li>server rejects the current token with either {@link TokenExpiredException} or
     *     {@link AuthenticationException} (see
     *     {@link AuthTokenManager#handleSecurityException(AuthToken, SecurityException)})</li>
     * </ol>
     * <p>
     * The supplier will be called by a task running in the {@link ForkJoinPool#commonPool()} as documented in the
     * {@link CompletableFuture#supplyAsync(Supplier)}.
     *
     * @param newTokenSupplier a new token supplier
     * @return a new token manager
     * @since 5.12
     */
    public static AuthTokenManager bearer(Supplier<AuthTokenAndExpiration> newTokenSupplier) {
        requireNonNull(newTokenSupplier, "newTokenSupplier must not be null");
        return bearerAsync(() -> CompletableFuture.supplyAsync(newTokenSupplier));
    }

    /**
     * Returns an {@link AuthTokenManager} that manages bearer {@link AuthToken} instances with UTC expiration
     * timestamp.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance. This includes the
     * following conditions:
     * <ol>
     *     <li>token's UTC timestamp is expired</li>
     *     <li>server rejects the current token with either {@link TokenExpiredException} or
     *     {@link AuthenticationException} (see
     *     {@link AuthTokenManager#handleSecurityException(AuthToken, SecurityException)})</li>
     * </ol>
     * <p>
     * The provided supplier and its completion stages must be non-blocking as documented in the {@link AuthTokenManager}.
     *
     * @param newTokenStageSupplier a new token stage supplier
     * @return a new token manager
     * @since 5.12
     */
    public static AuthTokenManager bearerAsync(
            Supplier<CompletionStage<AuthTokenAndExpiration>> newTokenStageSupplier) {
        requireNonNull(newTokenStageSupplier, "newTokenStageSupplier must not be null");
        return new ExpirationBasedAuthTokenManager(
                newTokenStageSupplier,
                Set.of(TokenExpiredException.class, AuthenticationException.class),
                Clock.systemUTC());
    }
}
