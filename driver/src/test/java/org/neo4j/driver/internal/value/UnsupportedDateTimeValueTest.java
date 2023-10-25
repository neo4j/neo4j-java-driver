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
package org.neo4j.driver.internal.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.openMocks;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.neo4j.driver.internal.types.InternalTypeSystem;

public class UnsupportedDateTimeValueTest {
    @Mock
    private DateTimeException exception;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
    }

    @MethodSource("throwingDateTimeAccessMethods")
    @ParameterizedTest
    void shouldThrowOnDateTimeAccess(Function<UnsupportedDateTimeValue, ?> throwingMethod) {
        // GIVEN
        given(exception.getMessage()).willReturn("message");
        var value = new UnsupportedDateTimeValue(exception);

        // WHEN
        var actualException = assertThrows(DateTimeException.class, () -> throwingMethod.apply(value));

        // THEN
        assertEquals(actualException.getMessage(), exception.getMessage());
        assertEquals(actualException.getCause(), exception);
    }

    static List<Arguments> throwingDateTimeAccessMethods() {
        return List.of(
                Arguments.of(Named.<Function<UnsupportedDateTimeValue, ?>>of(
                        "asOffsetDateTime", UnsupportedDateTimeValue::asOffsetDateTime)),
                Arguments.of(Named.<Function<UnsupportedDateTimeValue, ?>>of(
                        "asOffsetDateTime(OffsetDateTime)", v -> v.asOffsetDateTime(OffsetDateTime.now()))),
                Arguments.of(Named.<Function<UnsupportedDateTimeValue, ?>>of(
                        "asZonedDateTime", UnsupportedDateTimeValue::asZonedDateTime)),
                Arguments.of(Named.<Function<UnsupportedDateTimeValue, ?>>of(
                        "asZonedDateTime(ZonedDateTime)", v -> v.asZonedDateTime(ZonedDateTime.now()))),
                Arguments.of(Named.<Function<UnsupportedDateTimeValue, ?>>of(
                        "asObject", UnsupportedDateTimeValue::asObject)));
    }

    @Test
    @SuppressWarnings("EqualsWithItself")
    void shouldEqualToItself() {
        // GIVEN
        var value = new UnsupportedDateTimeValue(exception);

        // WHEN & THEN
        assertEquals(value, value);
    }

    @Test
    void shouldNotEqualToAnotherInstance() {
        // GIVEN
        var value0 = new UnsupportedDateTimeValue(exception);
        var value1 = new UnsupportedDateTimeValue(exception);

        // WHEN & THEN
        assertNotEquals(value0, value1);
    }

    @Test
    void shouldSupplyIdentityHashcode() {
        // GIVEN
        var value0 = new UnsupportedDateTimeValue(exception);
        var value1 = new UnsupportedDateTimeValue(exception);

        // WHEN & THEN
        assertNotEquals(value0.hashCode(), value1.hashCode());
    }

    @Test
    void shouldSupplyDateTimeType() {
        // GIVEN
        var value = new UnsupportedDateTimeValue(exception);

        // WHEN & THEN
        assertEquals(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), value.type());
    }
}
