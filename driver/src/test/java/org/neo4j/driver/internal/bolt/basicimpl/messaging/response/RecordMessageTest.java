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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.response;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

class RecordMessageTest {
    @ParameterizedTest
    @MethodSource("equalsArgs")
    void shouldEquals(RecordMessage message1, Object message2, boolean equals) {
        assertEquals(equals, message1.equals(message2));
    }

    static Stream<Arguments> equalsArgs() {
        var message = new RecordMessage(new Value[] {Values.value(1), Values.value("1")});
        return Stream.of(
                Arguments.of(message, new RecordMessage(new Value[] {Values.value(1), Values.value("1")}), true),
                Arguments.of(message, new RecordMessage(new Value[] {Values.value(2), Values.value("2")}), false),
                Arguments.of(message, new SuccessMessage(Collections.emptyMap()), false),
                Arguments.of(message, message, true),
                Arguments.of(message, null, false));
    }
}
