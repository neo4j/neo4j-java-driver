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
package org.neo4j.driver.internal.summary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationClassification;

class InternalNotificationTest {
    @ParameterizedTest
    @MethodSource("shouldMapArgs")
    void shouldMap(String value, NotificationCategory expectedNotificationCategory) {
        var notificationCategory = InternalNotification.valueOf(value).orElse(null);

        assertEquals(expectedNotificationCategory, notificationCategory);
    }

    static Stream<Arguments> shouldMapArgs() {
        return Arrays.stream(NotificationClassification.values())
                .map(notificationClassification -> switch (notificationClassification) {
                    case HINT -> Arguments.of(notificationClassification.toString(), NotificationCategory.HINT);
                    case UNRECOGNIZED -> Arguments.of(
                            notificationClassification.toString(), NotificationCategory.UNRECOGNIZED);
                    case UNSUPPORTED -> Arguments.of(
                            notificationClassification.toString(), NotificationCategory.UNSUPPORTED);
                    case PERFORMANCE -> Arguments.of(
                            notificationClassification.toString(), NotificationCategory.PERFORMANCE);
                    case DEPRECATION -> Arguments.of(
                            notificationClassification.toString(), NotificationCategory.DEPRECATION);
                    case SECURITY -> Arguments.of(notificationClassification.toString(), NotificationCategory.SECURITY);
                    case TOPOLOGY -> Arguments.of(notificationClassification.toString(), NotificationCategory.TOPOLOGY);
                    case GENERIC -> Arguments.of(notificationClassification.toString(), NotificationCategory.GENERIC);
                    case SCHEMA -> Arguments.of(notificationClassification.toString(), NotificationCategory.SCHEMA);
                });
    }
}
