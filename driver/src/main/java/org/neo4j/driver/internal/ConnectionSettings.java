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
package org.neo4j.driver.internal;

import org.neo4j.driver.AuthToken;

/**
 * The connection settings are used whenever a new connection is
 * established to a server, specifically as part of the INIT request.
 */
public class ConnectionSettings
{
    private final AuthToken authToken;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public ConnectionSettings( AuthToken authToken, String userAgent, int connectTimeoutMillis )
    {
        this.authToken = authToken;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public AuthToken authToken()
    {
        return authToken;
    }

    public String userAgent()
    {
        return userAgent;
    }

    public int connectTimeoutMillis()
    {
        return connectTimeoutMillis;
    }
}
