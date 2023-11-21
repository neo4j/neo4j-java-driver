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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.NotificationCategory;

class InternalNotificationCategoryTests {

    @ParameterizedTest
    @MethodSource("typeToCategoryMappings")
    void parseKnownCategories(TypeAndCategory typeAndCategory) {
        var parsedValue = InternalNotificationCategory.valueOf(typeAndCategory.type());

        assertTrue(parsedValue.isPresent());
        assertEquals(typeAndCategory.category(), parsedValue.get());
    }

    private static Stream<Arguments> typeToCategoryMappings() {
        return Arrays.stream(InternalNotificationCategory.Type.values()).map(type -> switch (type) {
            case HINT -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.HINT)));
            case UNRECOGNIZED -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.UNRECOGNIZED)));
            case UNSUPPORTED -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.UNSUPPORTED)));
            case PERFORMANCE -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.PERFORMANCE)));
            case DEPRECATION -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.DEPRECATION)));
            case SECURITY -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.SECURITY)));
            case TOPOLOGY -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.TOPOLOGY)));
            case GENERIC -> Arguments.of(
                    Named.of(type.toString(), new TypeAndCategory(type.toString(), NotificationCategory.GENERIC)));
        });
    }

    private record TypeAndCategory(String type, NotificationCategory category) {}

    @Test
    void shouldReturnEmptyWhenNoMatchFound() {
        var unknownCategory = "something";

        var parsedValue = InternalNotificationCategory.valueOf(unknownCategory);

        System.out.println(parsedValue);
    }
}
