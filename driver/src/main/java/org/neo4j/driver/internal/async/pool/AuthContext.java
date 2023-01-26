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
package org.neo4j.driver.internal.async.pool;

import static java.util.Objects.requireNonNull;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;

public class AuthContext {
    private final AuthTokenManager authTokenManager;
    private AuthToken authToken;
    private Long authTimestamp;
    private boolean pendingLogoff;

    public AuthContext(AuthTokenManager authTokenManager) {
        requireNonNull(authTokenManager, "authTokenProvider must not be null");
        this.authTokenManager = authTokenManager;
    }

    public void initiateAuth(AuthToken authToken) {
        requireNonNull(authToken, "authToken must not be null");
        this.authToken = authToken;
        authTimestamp = null;
        pendingLogoff = false;
    }

    public AuthToken getAuthToken() {
        return authToken;
    }

    public void finishAuth(long authTimestamp) {
        this.authTimestamp = authTimestamp;
    }

    public Long getAuthTimestamp() {
        return authTimestamp;
    }

    public void markPendingLogoff() {
        pendingLogoff = true;
    }

    public boolean isPendingLogoff() {
        return pendingLogoff;
    }

    public AuthTokenManager getAuthTokenManager() {
        return authTokenManager;
    }
}
