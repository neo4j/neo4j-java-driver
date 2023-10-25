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
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.InternalAuthTokenAndExpiration;

/**
 * Token for holding authentication details, such as <em>user name</em> and <em>password</em>.
 * Such a token is required by a {@link Driver} to authenticate with a Neo4j
 * instance.
 *
 * @see AuthTokens
 * @see GraphDatabase#driver(String, AuthToken)
 * @since 1.0
 */
public sealed interface AuthToken permits InternalAuthToken {
    /**
     * Returns a new instance of a type holding both the token and its UTC expiration timestamp.
     * <p>
     * This is used by the bearer token implementation of the {@link AuthTokenManager} supplied by the
     * {@link AuthTokenManagers}.
     *
     * @param utcExpirationTimestamp the UTC expiration timestamp
     * @return a new instance of a type holding both the token and its UTC expiration timestamp
     * @since 5.8
     * @see AuthTokenManagers#bearer(Supplier)
     * @see AuthTokenManagers#bearerAsync(Supplier)
     */
    default AuthTokenAndExpiration expiringAt(long utcExpirationTimestamp) {
        return new InternalAuthTokenAndExpiration(this, utcExpirationTimestamp);
    }
}
