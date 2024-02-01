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
package neo4j.org.testkit.backend;

import java.net.URI;
import java.util.logging.Level;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Logging;

public record Config(int port, URI uri, AuthToken authToken, Logging logging) {
    static Config load() {
        var env = System.getenv();
        var port = Integer.parseInt(env.getOrDefault("TEST_BACKEND_PORT", "9000"));
        var neo4jHost = env.getOrDefault("TEST_NEO4J_HOST", "localhost");
        var neo4jPort = Integer.parseInt(env.getOrDefault("TEST_NEO4J_PORT", "7687"));
        var neo4jScheme = env.getOrDefault("TEST_NEO4J_SCHEME", "neo4j");
        var neo4jUser = env.getOrDefault("TEST_NEO4J_USER", "neo4j");
        var neo4jPassword = env.getOrDefault("TEST_NEO4J_PASS", "password");
        var level = env.get("TEST_BACKEND_LOGGING_LEVEL");
        var logging = level == null || level.isEmpty() ? Logging.none() : Logging.console(Level.parse(level));
        return new Config(
                port,
                URI.create(String.format("%s://%s:%d", neo4jScheme, neo4jHost, neo4jPort)),
                AuthTokens.basic(neo4jUser, neo4jPassword),
                logging);
    }
}
