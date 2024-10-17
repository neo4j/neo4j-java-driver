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
import static org.neo4j.driver.internal.bolt.api.AccessMode.READ;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage.RESET;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.BeginMessage;

class BeginMessageEncoderTest {
    private final BeginMessageEncoder encoder = new BeginMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @ParameterizedTest
    @MethodSource("arguments")
    void shouldEncodeBeginMessage(AccessMode mode, String impersonatedUser, String txType) throws Exception {
        var bookmarks = Set.of("neo4j:bookmark:v1:tx42");

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("hello", value("world"));
        txMetadata.put("answer", value(42));

        var txTimeout = Duration.ofSeconds(1);

        var loggingProvider = new LoggingProvider() {
            @Override
            public System.Logger getLog(Class<?> cls) {
                return mock(System.Logger.class);
            }

            @Override
            public System.Logger getLog(String name) {
                return mock(System.Logger.class);
            }
        };

        encoder.encode(
                new BeginMessage(
                        bookmarks,
                        txTimeout,
                        txMetadata,
                        mode,
                        DatabaseNameUtil.defaultDatabase(),
                        impersonatedUser,
                        txType,
                        null,
                        false,
                        loggingProvider),
                packer);

        var order = inOrder(packer);
        order.verify(packer).packStructHeader(1, BeginMessage.SIGNATURE);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks", value(bookmarks.stream().map(Values::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", value(1000));
        expectedMetadata.put("tx_metadata", value(txMetadata));
        if (mode == READ) {
            expectedMetadata.put("mode", value("r"));
        }
        if (impersonatedUser != null) {
            expectedMetadata.put("imp_user", value(impersonatedUser));
        }
        if (txType != null) {
            expectedMetadata.put("tx_type", value(txType));
        }

        order.verify(packer).pack(expectedMetadata);
    }

    private static Stream<Arguments> arguments() {
        return Arrays.stream(AccessMode.values())
                .flatMap(accessMode ->
                        Stream.of(Arguments.of(accessMode, "user", "IMPLICIT"), Arguments.of(accessMode, null, null)));
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(RESET, packer));
    }
}
