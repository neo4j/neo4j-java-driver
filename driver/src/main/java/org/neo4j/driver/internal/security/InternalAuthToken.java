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
package org.neo4j.driver.internal.security;

import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Value;

/**
 * A simple common token for authentication schemes that easily convert to
 * an auth token map
 */
public final class InternalAuthToken implements AuthToken {
    public static final String SCHEME_KEY = "scheme";
    public static final String PRINCIPAL_KEY = "principal";
    public static final String CREDENTIALS_KEY = "credentials";
    public static final String REALM_KEY = "realm";
    public static final String PARAMETERS_KEY = "parameters";

    private final Map<String, Value> content;

    public InternalAuthToken(Map<String, Value> content) {
        this.content = content;
    }

    public Map<String, Value> toMap() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var that = (InternalAuthToken) o;

        return Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return content != null ? content.hashCode() : 0;
    }
}
