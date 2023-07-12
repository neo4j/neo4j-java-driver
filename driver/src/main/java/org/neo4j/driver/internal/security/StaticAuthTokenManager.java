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
package org.neo4j.driver.internal.security;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.TokenExpiredException;

public class StaticAuthTokenManager implements AuthTokenManager {
    private final AtomicBoolean expired = new AtomicBoolean();
    private final AuthToken authToken;

    public StaticAuthTokenManager(AuthToken authToken) {
        requireNonNull(authToken, "authToken must not be null");
        this.authToken = authToken;
    }

    @Override
    public CompletionStage<AuthToken> getToken() {
        return expired.get()
                ? CompletableFuture.failedFuture(new TokenExpiredException(null, "authToken is expired"))
                : CompletableFuture.completedFuture(authToken);
    }

    @Override
    public void onSecurityException(AuthToken authToken, SecurityException exception) {
        if (authToken.equals(this.authToken)) {
            expired.set(true);
        }
    }
}
