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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class BookmarkManagerConfigTest {
    BookmarkManagerConfig config;

    @Test
    void shouldReturnDefaultValues() {
        // GIVEN & WHEN
        config = BookmarkManagerConfig.builder().build();

        // THEN
        assertNotNull(config.initialBookmarks());
        assertTrue(config.initialBookmarks().isEmpty());
        assertTrue(config.bookmarksConsumer().isEmpty());
        assertTrue(config.bookmarksSupplier().isEmpty());
    }

    @Test
    void shouldReturnInitialBookmarks() {
        // GIVEN
        var bookmarks = Set.of(Bookmark.from("TS:000001"));

        // WHEN
        config = BookmarkManagerConfig.builder().withInitialBookmarks(bookmarks).build();

        // WHEN & THEN
        assertEquals(bookmarks, config.initialBookmarks());
        assertTrue(config.bookmarksConsumer().isEmpty());
        assertTrue(config.bookmarksSupplier().isEmpty());
    }

    @Test
    void shouldReturnUpdateListener() {
        // GIVEN
        Consumer<Set<Bookmark>> updateListener = (b) -> {};

        // WHEN
        config = BookmarkManagerConfig.builder()
                .withBookmarksConsumer(updateListener)
                .build();

        // WHEN & THEN
        assertNotNull(config.initialBookmarks());
        assertTrue(config.initialBookmarks().isEmpty());
        assertEquals(updateListener, config.bookmarksConsumer().orElse(null));
        assertTrue(config.bookmarksSupplier().isEmpty());
    }

    @Test
    void shouldReturnBookmarkSupplier() {
        // GIVEN
        @SuppressWarnings("unchecked")
        Supplier<Set<Bookmark>> bookmarkSupplier = mock(Supplier.class);

        // WHEN
        config = BookmarkManagerConfig.builder()
                .withBookmarksSupplier(bookmarkSupplier)
                .build();

        // WHEN & THEN
        assertNotNull(config.initialBookmarks());
        assertTrue(config.initialBookmarks().isEmpty());
        assertTrue(config.bookmarksConsumer().isEmpty());
        assertEquals(bookmarkSupplier, config.bookmarksSupplier().orElse(null));
    }
}
