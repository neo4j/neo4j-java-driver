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
package org.neo4j.driver.internal.adaptedbolt;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.pooled.AuthTokenManager;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.SecurityRetryableException;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.value.BoltValueFactory;

public final class BoltAuthTokenManager implements AuthTokenManager {
    private final org.neo4j.driver.AuthTokenManager authTokenManager;
    private final BoltValueFactory valueFactory;
    private final ErrorMapper errorMapper;

    public BoltAuthTokenManager(
            org.neo4j.driver.AuthTokenManager authTokenManager,
            BoltValueFactory valueFactory,
            ErrorMapper errorMapper) {
        this.authTokenManager = Objects.requireNonNull(authTokenManager);
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.errorMapper = Objects.requireNonNull(errorMapper);
    }

    @Override
    public CompletionStage<AuthToken> getToken() {
        return authTokenManager
                .getToken()
                .thenApply(authToken ->
                        AuthTokens.custom(valueFactory.toBoltMap(((InternalAuthToken) authToken).toMap())));
    }

    @Override
    public BoltFailureException handleBoltFailureException(AuthToken authToken, BoltFailureException exception) {
        var neo4jException = errorMapper.mapBoltFailureException(exception);
        if (neo4jException instanceof SecurityException securityException) {
            var mappedAuthToken =
                    new InternalAuthToken(BoltValueFactory.getInstance().toDriverMap(authToken.asMap()));
            if (authTokenManager.handleSecurityException(mappedAuthToken, securityException)) {
                neo4jException = new SecurityRetryableException(securityException);
            }
        }
        return new BoltFailureExceptionWithNeo4jException(exception, neo4jException);
    }
}
