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
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage.newDiscardAllMessage;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage;

class DiscardMessageEncoderTest {
    private final DiscardMessageEncoder encoder = new DiscardMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @Test
    void shouldDiscardAllCorrectly() throws Throwable {
        encoder.encode(newDiscardAllMessage(-1), packer);

        Map<String, Value> meta = new HashMap<>();
        meta.put("n", value(-1));

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, DiscardMessage.SIGNATURE);
        order.verify(packer).pack(meta);
    }

    @Test
    void shouldEncodeDiscardMessage() throws Exception {
        encoder.encode(new DiscardMessage(100, 200), packer);

        Map<String, Value> meta = new HashMap<>();
        meta.put("n", value(100));
        meta.put("qid", value(200));

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, DiscardMessage.SIGNATURE);
        order.verify(packer).pack(meta);
    }

    @Test
    void shouldAvoidQueryId() throws Throwable {
        encoder.encode(new DiscardMessage(100, -1), packer);

        Map<String, Value> meta = new HashMap<>();
        meta.put("n", value(100));

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, DiscardMessage.SIGNATURE);
        order.verify(packer).pack(meta);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(DiscardAllMessage.DISCARD_ALL, packer));
    }
}
