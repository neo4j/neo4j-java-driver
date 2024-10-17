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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.TelemetryMessage;

class TelemetryMessageEncoderTest {
    private final TelemetryMessageEncoder encoder = new TelemetryMessageEncoder();
    private final ValuePacker packer = mock(ValuePacker.class);

    @ParameterizedTest
    @MethodSource("validApis")
    void shouldEncodeTelemetryMessage(int api) throws Exception {
        encoder.encode(new TelemetryMessage(api), packer);

        verify(packer).packStructHeader(1, TelemetryMessage.SIGNATURE);
        verify(packer).pack(Values.value(api));
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encode(
                        RunWithMetadataMessage.unmanagedTxRunMessage("RETURN 2", Collections.emptyMap()), packer));
    }

    private static Stream<Integer> validApis() {
        return Stream.of(TelemetryApi.values()).map(TelemetryApi::getValue);
    }
}
