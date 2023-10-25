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
import static org.neo4j.driver.internal.InternalBookmark.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
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
        var one = parse("one");
        var two = parse("two");
        var config = builder().withBookmarks(one, two).build();
        assertEquals(Arrays.asList(one, two), config.bookmarks());

        var config2 = builder().withBookmarks(Arrays.asList(one, two)).build();
        assertEquals(Arrays.asList(one, two), config2.bookmarks());
    }

    @Test
    void shouldAcceptNullInBookmarks() {
        var one = parse("one");
        var two = parse("two");
        var config = builder().withBookmarks(one, two, null).build();
        assertEquals(Arrays.asList(one, two, null), config.bookmarks());

        var config2 = builder().withBookmarks(Arrays.asList(one, two, null)).build();
        assertEquals(Arrays.asList(one, two, null), config2.bookmarks());
    }

    @Test
    void shouldSaveBookmarksCopyFromArray() {
        var bookmark1 = parse("one");
        var bookmark2 = parse("two");
        var bookmarks = new Bookmark[] {bookmark1, bookmark2};
        var config = builder().withBookmarks(bookmarks).build();
        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());

        bookmarks[0] = parse("three");

        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());
    }

    @Test
    void shouldSaveBookmarksCopyFromIterable() {
        var bookmark1 = parse("one");
        var bookmark2 = parse("two");
        var bookmarks = new ArrayList<>(List.of(bookmark1, bookmark2));
        var config = builder().withBookmarks(bookmarks).build();
        assertEquals(List.of(bookmark1, bookmark2), config.bookmarks());

        bookmarks.add(parse("three"));

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
    @SuppressWarnings("deprecation")
    void shouldSerialize() throws Exception {
        var config = SessionConfig.builder()
                .withBookmarks(
                        Bookmark.from(new HashSet<>(Arrays.asList("bookmarkA", "bookmarkB"))),
                        Bookmark.from(new HashSet<>(Arrays.asList("bookmarkC", "bookmarkD"))))
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

        List<Set<String>> bookmarks = new ArrayList<>();
        verify.bookmarks().forEach(b -> bookmarks.add(b.values()));
        assertEquals(2, bookmarks.size());
        assertTrue(bookmarks.get(0).containsAll(Arrays.asList("bookmarkA", "bookmarkB")));
        assertTrue(bookmarks.get(1).containsAll(Arrays.asList("bookmarkC", "bookmarkD")));

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
}
