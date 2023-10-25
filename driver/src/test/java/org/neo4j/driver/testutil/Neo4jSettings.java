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
package org.neo4j.driver.testutil;

import java.util.Map;
import java.util.Set;

public class Neo4jSettings {
    public static final String SSL_POLICY_BOLT_ENABLED = "dbms.ssl.policy.bolt.enabled";
    public static final String SSL_POLICY_BOLT_CLIENT_AUTH = "dbms.ssl.policy.bolt.client_auth";
    public static final String BOLT_TLS_LEVEL = "dbms.connector.bolt.tls_level";

    static final int TEST_JVM_ID = Integer.getInteger("testJvmId", 0);

    private static final int DEFAULT_HTTP_PORT = 12000;
    private static final int DEFAULT_HTTPS_PORT = 13000;
    private static final int DEFAULT_BOLT_PORT = 14000;
    private static final int DEFAULT_DISCOVERY_LISTEN_PORT = 15000;
    private static final int DEFAULT_RAFT_ADVERTISED_PORT = 16000;

    private final Map<String, String> settings;
    private final Set<String> excludes;

    public enum BoltTlsLevel {
        OPTIONAL,
        REQUIRED,
        DISABLED
    }

    private Neo4jSettings(Map<String, String> settings, Set<String> excludes) {
        this.settings = settings;
        this.excludes = excludes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var that = (Neo4jSettings) o;

        if (!settings.equals(that.settings)) {
            return false;
        }
        return excludes.equals(that.excludes);
    }

    @Override
    public int hashCode() {
        return settings.hashCode();
    }
}
