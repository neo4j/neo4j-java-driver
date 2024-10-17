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

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RouteMessage;

class RouteMessageEncoderTest {
    private final ValuePacker packer = mock(ValuePacker.class);
    private final RouteMessageEncoder encoder = new RouteMessageEncoder();

    @ParameterizedTest
    @ValueSource(strings = {"neo4j"})
    @NullSource
    void shouldEncodeRouteMessage(String databaseName) throws IOException {
        var routingContext = getRoutingContext();

        encoder.encode(new RouteMessage(getRoutingContext(), Collections.emptySet(), databaseName, null), packer);

        var inOrder = inOrder(packer);

        inOrder.verify(packer).packStructHeader(3, (byte) 0x66);
        inOrder.verify(packer).pack(routingContext);
        inOrder.verify(packer).pack(value(emptyList()));
        inOrder.verify(packer).pack(databaseName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"neo4j"})
    @NullSource
    void shouldEncodeRouteMessageWithBookmark(String databaseName) throws IOException {
        var routingContext = getRoutingContext();
        var bookmark = "somebookmark";

        encoder.encode(
                new RouteMessage(getRoutingContext(), Collections.singleton(bookmark), databaseName, null), packer);

        var inOrder = inOrder(packer);

        inOrder.verify(packer).packStructHeader(3, (byte) 0x66);
        inOrder.verify(packer).pack(routingContext);
        inOrder.verify(packer).pack(value(Collections.singleton(Values.value(bookmark))));
        inOrder.verify(packer).pack(databaseName);
    }

    @Test
    void shouldThrowIllegalArgumentIfMessageIsNotRouteMessage() {
        var message = mock(Message.class);

        assertThrows(IllegalArgumentException.class, () -> encoder.encode(message, packer));
    }

    private Map<String, Value> getRoutingContext() {
        Map<String, Value> routingContext = new HashMap<>();
        routingContext.put("ip", value("127.0.0.1"));
        return routingContext;
    }
}
