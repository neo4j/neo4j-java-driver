/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.neo4j.driver.internal.security.TemporalAuthTokenManager;

/**
 * Implementations of {@link AuthTokenManager}.
 * @since 5.7
 */
public final class AuthTokenManagers {
    private AuthTokenManagers() {}

    /**
     * Returns an {@link AuthTokenManager} that manages {@link AuthToken} instances with UTC expiration timestamp.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance. This includes the
     * following conditions:
     * <ol>
     *     <li>token's UTC timestamp is expired</li>
     *     <li>server rejects the current token (see {@link AuthTokenManager#onExpired(AuthToken)})</li>
     * </ol>
     * <p>
     * The supplier will be called by a task running in the {@link ForkJoinPool#commonPool()} as documented in the
     * {@link CompletableFuture#supplyAsync(Supplier)}.
     *
     * @param newTokenSupplier a new token supplier
     * @return a new token manager
     */
    public static AuthTokenManager temporal(Supplier<TemporalAuthData> newTokenSupplier) {
        return new TemporalAuthTokenManager(() -> CompletableFuture.supplyAsync(newTokenSupplier), Clock.systemUTC());
    }

    /**
     * Returns an {@link AuthTokenManager} that manages {@link AuthToken} instances with UTC expiration timestamp.
     * <p>
     * The implementation will only use the token supplier when it needs a new token instance. This includes the
     * following conditions:
     * <ol>
     *     <li>token's UTC timestamp is expired</li>
     *     <li>server rejects the current token (see {@link AuthTokenManager#onExpired(AuthToken)})</li>
     * </ol>
     * <p>
     * The provided supplier and its completion stages must be non-blocking as documented in the {@link AuthTokenManager}.
     *
     * @param newTokenStageSupplier a new token stage supplier
     * @return a new token manager
     */
    public static AuthTokenManager temporalAsync(Supplier<CompletionStage<TemporalAuthData>> newTokenStageSupplier) {
        return new TemporalAuthTokenManager(newTokenStageSupplier, Clock.systemUTC());
    }

    /**
     * A container used by the temporal {@link AuthTokenManager} implementation provided by the driver, it contains an
     * {@link AuthToken} and its UTC expiration timestamp.
     * @since 5.7
     */
    public interface TemporalAuthData {
        /**
         * Returns a new instance with the provided token and {@link Long#MAX_VALUE} expiration timestamp.
         * @param authToken the token
         * @return a new instance
         */
        static TemporalAuthData of(AuthToken authToken) {
            return of(authToken, Long.MAX_VALUE);
        }

        /**
         * Returns a new instance with the provided token and its expiration timestamp.
         * @param authToken the token
         * @param expirationTimestamp the expiration timestamp
         * @return a new instance
         */
        static TemporalAuthData of(AuthToken authToken, long expirationTimestamp) {
            return new TemporalAuthTokenManager.InternalTemporalAuthData(authToken, expirationTimestamp);
        }

        /**
         * Returns the {@link AuthToken}.
         *
         * @return the token
         */
        AuthToken authToken();

        /**
         * Returns the token's UTC expiration timestamp.
         *
         * @return the token's UTC expiration timestamp
         */
        long expirationTimestamp();
    }
}
