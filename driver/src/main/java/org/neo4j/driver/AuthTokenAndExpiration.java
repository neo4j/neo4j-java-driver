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

import java.util.function.Supplier;
import org.neo4j.driver.internal.security.InternalAuthTokenAndExpiration;

/**
 * A container used by the expiration based {@link AuthTokenManager} implementation provided by the driver, it contains an
 * {@link AuthToken} and its UTC expiration timestamp.
 * <p>
 * This is used by the bearer token implementation of the {@link AuthTokenManager} supplied by the
 * {@link AuthTokenManagers}.
 *
 * @since 5.8
 * @see AuthTokenManagers#bearer(Supplier)
 * @see AuthTokenManagers#bearerAsync(Supplier)
 */
public sealed interface AuthTokenAndExpiration permits InternalAuthTokenAndExpiration {
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
