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
package org.neo4j.driver.internal;

import org.neo4j.driver.AuthTokenManager;

/**
 * The connection settings are used whenever a new connection is
 * established to a server, specifically as part of the INIT request.
 */
public class ConnectionSettings {
    private final AuthTokenManager authTokenManager;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public ConnectionSettings(AuthTokenManager authTokenManager, String userAgent, int connectTimeoutMillis) {
        this.authTokenManager = authTokenManager;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public AuthTokenManager authTokenProvider() {
        return authTokenManager;
    }

    public String userAgent() {
        return userAgent;
    }

    public int connectTimeoutMillis() {
        return connectTimeoutMillis;
    }
}
