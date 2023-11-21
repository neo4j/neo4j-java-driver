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
package org.neo4j.driver.internal.async.pool;

import static java.util.Objects.requireNonNull;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;

public class AuthContext {
    private final AuthTokenManager authTokenManager;
    private AuthToken authToken;
    private Long authTimestamp;
    private boolean pendingLogoff;
    private boolean managed;
    private AuthToken validToken;

    public AuthContext(AuthTokenManager authTokenManager) {
        requireNonNull(authTokenManager, "authTokenProvider must not be null");
        this.authTokenManager = authTokenManager;
        this.managed = true;
    }

    public void initiateAuth(AuthToken authToken) {
        initiateAuth(authToken, true);
    }

    public void initiateAuth(AuthToken authToken, boolean managed) {
        requireNonNull(authToken, "authToken must not be null");
        this.authToken = authToken;
        authTimestamp = null;
        pendingLogoff = false;
        this.managed = managed;
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

    public void setValidToken(AuthToken validToken) {
        this.validToken = validToken;
    }

    public AuthToken getValidToken() {
        return validToken;
    }

    public boolean isManaged() {
        return managed;
    }

    public AuthTokenManager getAuthTokenManager() {
        return authTokenManager;
    }
}
