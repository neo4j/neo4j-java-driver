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
package org.neo4j.driver.internal.encryption;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.types.TypeSystem;

class InternalPropertyEncryptionRequestTest {
    static TypeSystem TYPE_SYSTEM = TypeSystem.getDefault();

    @ParameterizedTest
    @MethodSource("supportedValuesArgs")
    void shouldSupportValue(Value value) {
        var request = PropertyEncryptionRequest.builder()
                .fromValue(value)
                .usingKeyAlias("main-key")
                .build();
        assertNotNull(request);
    }

    @ParameterizedTest
    @MethodSource("unsupportedValuesArgs")
    void shouldNotSupportValue(Value value) {
        assertThrows(IllegalArgumentException.class, () -> PropertyEncryptionRequest.builder()
                .fromValue(value)
                .usingKeyAlias("main-key")
                .build());
    }

    @ParameterizedTest
    @MethodSource("supportedAADArgs")
    void shouldSupportAAD(Value value) {
        var request = PropertyEncryptionRequest.builder()
                .fromValue(0)
                .withAAD(value)
                .usingKeyAlias("main-key")
                .build();
        assertNotNull(request);
    }

    @ParameterizedTest
    @MethodSource("unsupportedAADArgs")
    void shouldNotSupportAAD(Value value) {
        assertThrows(IllegalArgumentException.class, () -> PropertyEncryptionRequest.builder()
                .fromValue(0)
                .withAAD(value)
                .usingKeyAlias("main-key")
                .build());
    }

    static Stream<Arguments> supportedValuesArgs() {
        return Stream.concat(Stream.concat(supportedAADs(), supportedPropertiesNotSupportedAsAAD()), supportedLists())
                .map(Arguments::of);
    }

    static Stream<Arguments> unsupportedValuesArgs() {
        return Stream.concat(unsupportedValues(), unsupportedLists()).map(Arguments::of);
    }

    static Stream<Arguments> supportedAADArgs() {
        return supportedAADs().map(Arguments::of);
    }

    static Stream<Arguments> unsupportedAADArgs() {
        return Stream.concat(
                        Stream.concat(unsupportedLists(), unsupportedValues()), supportedPropertiesNotSupportedAsAAD())
                .map(Arguments::of);
    }

    static Stream<Value> supportedPropertiesNotSupportedAsAAD() {
        return Stream.of(
                Values.value((Object) null),
                Values.value(Double.MAX_VALUE),
                Values.value(Double.MIN_VALUE),
                Values.value(ZonedDateTime.now()),
                Values.value(LocalDateTime.now()),
                Values.isoDuration(0, 0, 0, 0),
                Values.value(Duration.ZERO),
                Values.value(Period.ofYears(1000)),
                Values.vector(new long[] {0}));
    }

    static Stream<Value> supportedAADs() {
        return Stream.of(
                Values.value(false),
                Values.value(true),
                Values.value(new byte[] {0}),
                Values.value("value"),
                Values.value(Long.MAX_VALUE),
                Values.value(Long.MIN_VALUE),
                Values.point(0, 0, 0),
                Values.point(0, 0, 0, 0),
                Values.value(LocalDate.now()),
                Values.value(OffsetTime.now()),
                Values.value(LocalTime.now()),
                Values.value(UUID.randomUUID()));
    }

    static Stream<Value> supportedLists() {
        return Stream.concat(
                Stream.concat(supportedAADs(), supportedPropertiesNotSupportedAsAAD())
                        .filter(value -> !(TYPE_SYSTEM.NULL().isTypeOf(value)
                                || TYPE_SYSTEM.VECTOR().isTypeOf(value)))
                        .map(value -> Values.value(List.of(value, value))),
                Stream.of(Values.value(List.of())));
    }

    static Stream<Value> unsupportedLists() {
        var listWithNull = new ArrayList<>();
        listWithNull.add(null);
        return Stream.of(
                Values.value(listWithNull),
                Values.value(List.of(Values.NULL)),
                Values.value(List.of(Values.value(Long.MAX_VALUE), Values.value(Double.MAX_VALUE))),
                Values.value(List.of(Values.point(0, 0, 0), Values.point(0, 0, 0, 0))),
                Values.value(List.of(Values.point(0, 0, 0), Values.point(7, 0, 0))),
                Values.value(List.of(Values.point(0, 0, 0, 0), Values.point(7, 0, 0, 0))),
                Values.value(List.of(Values.vector(new long[] {0}), Values.vector(new long[] {0}))),
                Values.value(List.of(Map.of())));
    }

    static Stream<Value> unsupportedValues() {
        return Stream.of(Values.value(Map.of()));
    }
}
