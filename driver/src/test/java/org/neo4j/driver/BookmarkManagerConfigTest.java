/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
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
        assertNull(config.updateListener());
        assertNull(config.bookmarkSupplier());
    }

    @Test
    void shouldReturnInitialBookmarks() {
        // GIVEN
        var bookmarks = Map.of("neo4j", Set.of(Bookmark.from("TS:000001")));

        // WHEN
        config = BookmarkManagerConfig.builder().withInitialBookmarks(bookmarks).build();

        // WHEN & THEN
        assertEquals(bookmarks, config.initialBookmarks());
        assertNull(config.updateListener());
        assertNull(config.bookmarkSupplier());
    }

    @Test
    void shouldReturnUpdateListener() {
        // GIVEN
        BiConsumer<String, Set<Bookmark>> updateListener = (d, b) -> {};

        // WHEN
        config = BookmarkManagerConfig.builder()
                .withBookmarkConsumer(updateListener)
                .build();

        // WHEN & THEN
        assertNotNull(config.initialBookmarks());
        assertTrue(config.initialBookmarks().isEmpty());
        assertEquals(updateListener, config.updateListener());
        assertNull(config.bookmarkSupplier());
    }

    @Test
    void shouldReturnBookmarkSupplier() {
        // GIVEN
        var bookmarkSupplier = mock(BookmarkSupplier.class);

        // WHEN
        config = BookmarkManagerConfig.builder()
                .withBookmarkSupplier(bookmarkSupplier)
                .build();

        // WHEN & THEN
        assertNotNull(config.initialBookmarks());
        assertTrue(config.initialBookmarks().isEmpty());
        assertNull(config.updateListener());
        assertEquals(bookmarkSupplier, config.bookmarkSupplier());
    }
}
