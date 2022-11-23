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
package org.neo4j.driver.util;

import static org.neo4j.driver.internal.util.Iterables.map;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Neo4jSettings {
    public static final String DATA_DIR = "dbms.directories.data";
    public static final String IMPORT_DIR = "dbms.directories.import";
    public static final String SSL_POLICY_BOLT_ENABLED = "dbms.ssl.policy.bolt.enabled";
    public static final String SSL_POLICY_BOLT_CLIENT_AUTH = "dbms.ssl.policy.bolt.client_auth";
    // 5.0
    public static final String LISTEN_ADDR = "dbms.default_listen_address";
    public static final String IPV6_ENABLED_ADDR = "::";
    public static final String BOLT_TLS_LEVEL = "dbms.connector.bolt.tls_level";

    private static final String DEFAULT_IMPORT_DIR = "import";
    public static final String DEFAULT_BOLT_TLS_LEVEL = BoltTlsLevel.OPTIONAL.toString();

    public static final String DEFAULT_DATA_DIR = "data";

    static final int TEST_JVM_ID = Integer.getInteger("testJvmId", 0);

    private static final int DEFAULT_HTTP_PORT = 12000;
    private static final int DEFAULT_HTTPS_PORT = 13000;
    private static final int DEFAULT_BOLT_PORT = 14000;
    private static final int DEFAULT_DISCOVERY_LISTEN_PORT = 15000;
    private static final int DEFAULT_RAFT_ADVERTISED_PORT = 16000;
    private static final int DEFAULT_TX_LISTEN_PORT = 17000;

    static final int CURRENT_HTTP_PORT = DEFAULT_HTTP_PORT + TEST_JVM_ID;
    private static final int CURRENT_HTTPS_PORT = DEFAULT_HTTPS_PORT + TEST_JVM_ID;
    static final int CURRENT_BOLT_PORT = DEFAULT_BOLT_PORT + TEST_JVM_ID;
    static final int CURRENT_DISCOVERY_LISTEN_PORT = DEFAULT_DISCOVERY_LISTEN_PORT + TEST_JVM_ID;
    static final int CURRENT_RAFT_ADVERTISED_PORT = DEFAULT_RAFT_ADVERTISED_PORT + TEST_JVM_ID;
    static final int CURRENT_TX_LISTEN_PORT = DEFAULT_TX_LISTEN_PORT + TEST_JVM_ID;

    private static final String WINDOWS_SERVICE_NAME = "neo4j-" + TEST_JVM_ID;

    private final Map<String, String> settings;
    private final Set<String> excludes;

    public static final Neo4jSettings TEST_SETTINGS = new Neo4jSettings(
            map(
                    "dbms.connector.http.listen_address",
                    ":" + CURRENT_HTTP_PORT,
                    "dbms.connector.https.listen_address",
                    ":" + CURRENT_HTTPS_PORT,
                    "dbms.connector.bolt.listen_address",
                    ":" + CURRENT_BOLT_PORT,
                    "dbms.cluster.discovery.initial_members",
                    "localhost:" + CURRENT_DISCOVERY_LISTEN_PORT,
                    "server.discovery.listen_address",
                    ":" + CURRENT_DISCOVERY_LISTEN_PORT,
                    "cluster.raft_advertised_address",
                    ":" + CURRENT_RAFT_ADVERTISED_PORT,
                    "cluster.raft_listen_address",
                    ":" + CURRENT_RAFT_ADVERTISED_PORT,
                    "cluster.transaction_listen_address",
                    ":" + CURRENT_TX_LISTEN_PORT,
                    "cluster.transaction_advertised_address",
                    ":" + CURRENT_TX_LISTEN_PORT,
                    "server.cluster.advertised_address",
                    ":" + CURRENT_TX_LISTEN_PORT,
                    "dbms.windows_service_name",
                    WINDOWS_SERVICE_NAME,
                    DATA_DIR,
                    DEFAULT_DATA_DIR,
                    IMPORT_DIR,
                    DEFAULT_IMPORT_DIR,
                    BOLT_TLS_LEVEL,
                    DEFAULT_BOLT_TLS_LEVEL,
                    LISTEN_ADDR,
                    IPV6_ENABLED_ADDR),
            Collections.emptySet());

    public enum BoltTlsLevel {
        OPTIONAL,
        REQUIRED,
        DISABLED
    }

    private Neo4jSettings(Map<String, String> settings, Set<String> excludes) {
        this.settings = settings;
        this.excludes = excludes;
    }

    public Map<String, String> propertiesMap() {
        return settings;
    }

    public Neo4jSettings without(String key) {
        Set<String> newExcludes = new HashSet<>(excludes);
        newExcludes.add(key);
        Map<String, String> newMap = new HashMap<>(this.settings);
        newMap.remove(key);
        Neo4jSettings newSettings = new Neo4jSettings(newMap, newExcludes);
        return newSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Neo4jSettings that = (Neo4jSettings) o;

        if (!settings.equals(that.settings)) {
            return false;
        }
        return excludes.equals(that.excludes);
    }

    @Override
    public int hashCode() {
        return settings.hashCode();
    }

    public Set<String> excludes() {
        return excludes;
    }
}
