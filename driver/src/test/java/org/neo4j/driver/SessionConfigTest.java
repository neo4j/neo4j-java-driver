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
package org.neo4j.driver;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.SessionConfig.defaultConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.testutil.TestUtil;

class SessionConfigTest {
    @Test
    void shouldReturnDefaultValues() {
        var config = defaultConfig();

        assertEquals(AccessMode.WRITE, config.defaultAccessMode());
        assertFalse(config.database().isPresent());
        assertNull(config.bookmarks());
        assertFalse(config.fetchSize().isPresent());
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldChangeAccessMode(AccessMode mode) {
        var config = builder().withDefaultAccessMode(mode).build();
        assertEquals(mode, config.defaultAccessMode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo", "data", "my awesome database", "    "})
    void shouldChangeDatabaseName(String databaseName) {
        var config = builder().withDatabase(databaseName).build();
        assertTrue(config.database().isPresent());
        assertEquals(databaseName, config.database().get());
    }

    @Test
    void shouldNotAllowNullDatabaseName() {
        assertThrows(NullPointerException.class, () -> builder().withDatabase(null));
    }

    @ParameterizedTest
    @MethodSource("someConfigs")
    void nullDatabaseNameMustNotBreakEquals(SessionConfig config1, SessionConfig config2, boolean expectedEquals) {

        assertEquals(config1.equals(config2), expectedEquals);
    }

    static Stream<Arguments> someConfigs() {
        return Stream.of(
                arguments(
                        SessionConfig.builder().build(), SessionConfig.builder().build(), true),
                arguments(
                        SessionConfig.builder().withDatabase("a").build(),
                        SessionConfig.builder().build(),
                        false),
                arguments(
                        SessionConfig.builder().build(),
                        SessionConfig.builder().withDatabase("a").build(),
                        false),
                arguments(
                        SessionConfig.builder().withDatabase("a").build(),
                        SessionConfig.builder().withDatabase("a").build(),
                        true));
    }

    @ParameterizedTest
    @ValueSource(strings = {""})
    void shouldForbiddenEmptyStringDatabaseName(String databaseName) {
        var error = assertThrows(IllegalArgumentException.class, () -> builder().withDatabase(databaseName));
        assertTrue(error.getMessage().startsWith("Illegal database name "));
    }

    @Test
    void shouldAcceptNullBookmarks() {
        var config = builder().withBookmarks((Bookmark[]) null).build();
        assertNull(config.bookmarks());

        var config2 = builder().withBookmarks((List<Bookmark>) null).build();
        assertNull(config2.bookmarks());
    }

    @Test
    void shouldAcceptEmptyBookmarks() {
        var config = builder().withBookmarks().build();
        assertEquals(emptyList(), config.bookmarks());

        var config2 = builder().withBookmarks(emptyList()).build();
        assertEquals(emptyList(), config2.bookmarks());
    }

    @Test
    void shouldAcceptBookmarks() {
        var one = Bookmark.from("one");
        var two = Bookmark.from("two");
        var config = builder().withBookmarks(one, two).build();
        assertEquals(Arrays.asList(one, two), config.bookmarks());

        var config2 = builder().withBookmarks(Arrays.asList(one, two)).build();
        assertEquals(Arrays.asList(one, two), config2.bookmarks());
    }

    @Test
    void shouldAcceptNullInBookmarks() {
        var one = Bookmark.from("one");
        var two = Bookmark.from("two");
        var config = builder().withBookmarks(one, two, null).build();
        assertEquals(Arrays.asList(one, two, null), config.bookmarks());

        var config2 = builder().withBookmarks(Arrays.asList(one, two, null)).build();
        assertEquals(Arrays.asList(one, two, null), config2.bookmarks());
    }

    @Test
    void shouldSaveBookmarksCopyFromArray() {
        var bookmark1 = Bookmark.from("one");
        var bookmark2 = Bookmark.from("two");
        var bookmarks = new Bookmark[] {bookmark1, bookmark2};
        var config = builder().withBookmarks(bookmarks).build();
        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());

        bookmarks[0] = Bookmark.from("three");

        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());
    }

    @Test
    void shouldSaveBookmarksCopyFromIterable() {
        var bookmark1 = Bookmark.from("one");
        var bookmark2 = Bookmark.from("two");
        var bookmarks = new ArrayList<>(List.of(bookmark1, bookmark2));
        var config = builder().withBookmarks(bookmarks).build();
        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());

        bookmarks.add(Bookmark.from("three"));

        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());
    }

    @ParameterizedTest
    @ValueSource(longs = {100, 1, 1000, Long.MAX_VALUE, -1})
    void shouldChangeFetchSize(long value) {
        var config = builder().withFetchSize(value).build();
        assertEquals(Optional.of(value), config.fetchSize());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -100, -2})
    void shouldErrorWithIllegalFetchSize(long value) {
        assertThrows(
                IllegalArgumentException.class,
                () -> builder().withFetchSize(value).build());
    }

    @Test
    void shouldTwoConfigBeEqual() {
        var config1 = builder().withFetchSize(100).build();
        var config2 = builder().withFetchSize(100).build();

        assertEquals(config1, config2);
    }

    @Test
    void shouldSerialize() throws Exception {
        var bookmarks = Set.of(
                Bookmark.from("bookmarkA"),
                Bookmark.from("bookmarkB"),
                Bookmark.from("bookmarkC"),
                Bookmark.from("bookmarkD"));
        var config = SessionConfig.builder()
                .withBookmarks(bookmarks)
                .withDefaultAccessMode(AccessMode.WRITE)
                .withFetchSize(54321L)
                .withDatabase("testing")
                .withImpersonatedUser("impersonator")
                .withNotificationConfig(NotificationConfig.defaultConfig()
                        .enableMinimumSeverity(NotificationSeverity.WARNING)
                        .disableCategories(Set.of(NotificationCategory.UNSUPPORTED, NotificationCategory.UNRECOGNIZED)))
                .build();

        var verify = TestUtil.serializeAndReadBack(config, SessionConfig.class);

        assertNotNull(verify.bookmarks());

        assertEquals(
                bookmarks,
                StreamSupport.stream(verify.bookmarks().spliterator(), false).collect(Collectors.toUnmodifiableSet()));

        assertEquals(config.defaultAccessMode(), verify.defaultAccessMode());
        assertEquals(config.fetchSize(), verify.fetchSize());
        assertEquals(config.database(), verify.database());
        assertEquals(config.impersonatedUser(), verify.impersonatedUser());
        assertEquals(
                NotificationConfig.defaultConfig()
                        .enableMinimumSeverity(NotificationSeverity.WARNING)
                        .disableCategories(Set.of(NotificationCategory.UNSUPPORTED, NotificationCategory.UNRECOGNIZED)),
                config.notificationConfig());
    }

    @Test
    void shouldNotHaveMinimumNotificationSeverity() {
        var config = builder().build();

        assertTrue(config.minimumNotificationSeverity().isEmpty());
    }

    @Test
    void shouldSetMinimumNotificationSeverity() {
        var config = Config.builder()
                .withMinimumNotificationSeverity(NotificationSeverity.WARNING)
                .build();

        assertEquals(
                NotificationSeverity.WARNING,
                config.minimumNotificationSeverity().orElse(null));
    }

    @Test
    void shouldNotHaveDisabledNotificationClassifications() {
        var config = builder().build();

        assertTrue(config.disabledNotificationClassifications().isEmpty());
    }

    @Test
    void shouldSetDisabledNotificationClassifications() {
        var config = Config.builder()
                .withDisabledNotificationClassifications(Set.of(NotificationClassification.SECURITY))
                .build();

        assertEquals(Set.of(NotificationClassification.SECURITY), config.disabledNotificationClassifications());
    }
}
