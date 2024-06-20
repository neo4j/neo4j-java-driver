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
package org.neo4j.driver.internal.messaging.encode;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;

class RunWithMetadataMessageEncoderTest {
    private final RunWithMetadataMessageEncoder encoder = new RunWithMetadataMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldEncodeRunWithMetadataMessage(AccessMode mode) throws Exception {
        var params = singletonMap("answer", value(42));

        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx999"));

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("key1", value("value1"));
        txMetadata.put("key2", value(1, 2, 3, 4, 5));
        txMetadata.put("key3", value(true));

        var txTimeout = Duration.ofMillis(42);

        var query = new Query("RETURN $answer", value(params));
        encoder.encode(
                autoCommitTxRunMessage(
                        query,
                        txTimeout,
                        txMetadata,
                        defaultDatabase(),
                        mode,
                        bookmarks,
                        null,
                        null,
                        true,
                        Logging.none()),
                packer);

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(3, RunWithMetadataMessage.SIGNATURE);
        order.verify(packer).pack("RETURN $answer");
        order.verify(packer).pack(params);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks", value(bookmarks.stream().map(Bookmark::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", value(42));
        expectedMetadata.put("tx_metadata", value(txMetadata));
        if (mode == READ) {
            expectedMetadata.put("mode", value("r"));
        }

        order.verify(packer).pack(expectedMetadata);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(DISCARD_ALL, packer));
    }
}
