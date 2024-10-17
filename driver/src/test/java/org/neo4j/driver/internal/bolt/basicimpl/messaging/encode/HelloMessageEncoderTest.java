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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.encode;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage.PULL_ALL;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.BoltAgentUtil;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;

class HelloMessageEncoderTest {
    private final HelloMessageEncoder encoder = new HelloMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @Test
    void shouldEncodeHelloMessage() throws Exception {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("username", value("bob"));
        authToken.put("password", value("secret"));

        encoder.encode(new HelloMessage("MyDriver", BoltAgentUtil.VALUE, authToken, null, false, null, false), packer);

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, HelloMessage.SIGNATURE);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver"));
        expectedMetadata.put("bolt_agent", value(Map.of("product", BoltAgentUtil.VALUE.product())));
        order.verify(packer).pack(expectedMetadata);
    }

    @Test
    void shouldEncodeHelloMessageWithRoutingContext() throws Exception {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("username", value("bob"));
        authToken.put("password", value("secret"));

        Map<String, String> routingContext = new HashMap<>();
        routingContext.put("policy", "eu-fast");

        encoder.encode(
                new HelloMessage("MyDriver", BoltAgentUtil.VALUE, authToken, routingContext, false, null, false),
                packer);

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, HelloMessage.SIGNATURE);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", value("MyDriver"));
        expectedMetadata.put("bolt_agent", value(Map.of("product", BoltAgentUtil.VALUE.product())));
        expectedMetadata.put("routing", value(routingContext));
        order.verify(packer).pack(expectedMetadata);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(PULL_ALL, packer));
    }
}
