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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.request;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.PRINCIPAL_KEY;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltAgentUtil;

class HelloMessageTest {
    @Test
    void shouldHaveCorrectMetadata() {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("user", value("Alice"));
        authToken.put("credentials", value("SecretPassword"));

        var message = new HelloMessage(
                "MyDriver/1.0.2", BoltAgentUtil.VALUE, authToken, Collections.emptyMap(), false, null, false);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver/1.0.2"));
        expectedMetadata.put("bolt_agent", value(Map.of("product", BoltAgentUtil.VALUE.product())));
        expectedMetadata.put("routing", value(Collections.emptyMap()));
        assertEquals(expectedMetadata, message.metadata());
    }

    @Test
    void shouldHaveCorrectRoutingContext() {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("user", value("Alice"));
        authToken.put("credentials", value("SecretPassword"));

        Map<String, String> routingContext = new HashMap<>();
        routingContext.put("region", "China");
        routingContext.put("speed", "Slow");

        var message =
                new HelloMessage("MyDriver/1.0.2", BoltAgentUtil.VALUE, authToken, routingContext, false, null, false);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver/1.0.2"));
        expectedMetadata.put("bolt_agent", value(Map.of("product", BoltAgentUtil.VALUE.product())));
        expectedMetadata.put("routing", value(routingContext));
        assertEquals(expectedMetadata, message.metadata());
    }

    @Test
    void shouldNotExposeCredentialsInToString() {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put(PRINCIPAL_KEY, value("Alice"));
        authToken.put(CREDENTIALS_KEY, value("SecretPassword"));

        var message = new HelloMessage(
                "MyDriver/1.0.2", BoltAgentUtil.VALUE, authToken, Collections.emptyMap(), false, null, false);

        assertThat(message.toString(), not(containsString("SecretPassword")));
    }

    @Test
    void shouldAcceptNullBoltAgent() {
        var authToken = new HashMap<String, Value>();
        authToken.put("user", value("Alice"));
        authToken.put("credentials", value("SecretPassword"));

        var message = new HelloMessage("MyDriver/1.0.2", null, authToken, Collections.emptyMap(), false, null, false);

        var expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver/1.0.2"));
        expectedMetadata.put("routing", value(Collections.emptyMap()));
        assertEquals(expectedMetadata, message.metadata());
    }

    @Test
    void shouldAcceptDetailedBoltAgent() {
        var authToken = new HashMap<String, Value>();
        authToken.put("user", value("Alice"));
        authToken.put("credentials", value("SecretPassword"));
        var boltAgent = new BoltAgent("1", "2", "3", "4");

        var message =
                new HelloMessage("MyDriver/1.0.2", boltAgent, authToken, Collections.emptyMap(), false, null, false);

        var expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver/1.0.2"));
        expectedMetadata.put(
                "bolt_agent",
                value(Map.of(
                        "product",
                        boltAgent.product(),
                        "platform",
                        boltAgent.platform(),
                        "language",
                        boltAgent.language(),
                        "language_details",
                        boltAgent.languageDetails())));
        expectedMetadata.put("routing", value(Collections.emptyMap()));
        assertEquals(expectedMetadata, message.metadata());
    }
}
