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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage;

class DiscardAllMessageEncoderTest {
    private final DiscardAllMessageEncoder encoder = new DiscardAllMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @Test
    void shouldEncodeDiscardAllMessage() throws Exception {
        encoder.encode(DiscardAllMessage.DISCARD_ALL, packer);

        verify(packer).packStructHeader(0, DiscardAllMessage.SIGNATURE);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(new DiscardMessage(100, 200), packer));
    }
}
