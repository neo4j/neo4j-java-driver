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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarksSupplier;

class Neo4jBookmarkManagerTest {
    Neo4jBookmarkManager manager;

    @Test
    void shouldRejectNullInitialBookmarks() {
        assertThrows(NullPointerException.class, () -> new Neo4jBookmarkManager(null, null, null));
    }

    @Test
    void shouldAddInitialBookmarks() {
        // GIVEN
        var systemBookmarks = Set.of(Bookmark.from("SY:000001"));
        var neo4jDatabase = "neo4j";
        var neo4jBookmarks = Set.of(Bookmark.from("NE:000001"));
        var initialBookmarks =
                Map.of(DatabaseNameUtil.SYSTEM_DATABASE_NAME, systemBookmarks, neo4jDatabase, neo4jBookmarks);
        manager = new Neo4jBookmarkManager(initialBookmarks, null, null);

        // WHEN & THEN
        assertEquals(
                initialBookmarks.values().stream().flatMap(Set::stream).collect(Collectors.toSet()),
                manager.getAllBookmarks());
        assertEquals(systemBookmarks, manager.getBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME));
        assertEquals(neo4jBookmarks, manager.getBookmarks(neo4jDatabase));
    }

    @Test
    void shouldNotifyUpdateListener() {
        // GIVEN
        @SuppressWarnings("unchecked")
        BiConsumer<String, Set<Bookmark>> updateListener = mock(BiConsumer.class);
        manager = new Neo4jBookmarkManager(Collections.emptyMap(), updateListener, null);
        var bookmark = Bookmark.from("SY:000001");

        // WHEN
        manager.updateBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME, Collections.emptySet(), Set.of(bookmark));

        // THEN
        then(updateListener).should().accept(DatabaseNameUtil.SYSTEM_DATABASE_NAME, Set.of(bookmark));
    }

    @Test
    void shouldUpdateBookmarks() {
        // GIVEN
        var initialBookmark0 = Bookmark.from("SY:000001");
        var initialBookmark1 = Bookmark.from("SY:000002");
        var initialBookmark2 = Bookmark.from("SY:000003");
        var initialBookmark3 = Bookmark.from("SY:000004");
        var initialBookmark4 = Bookmark.from("SY:000005");
        var initialBookmarks = Map.of(
                DatabaseNameUtil.SYSTEM_DATABASE_NAME,
                Set.of(initialBookmark0, initialBookmark1, initialBookmark2, initialBookmark3, initialBookmark4));
        manager = new Neo4jBookmarkManager(initialBookmarks, null, null);
        var newBookmark = Bookmark.from("SY:000007");

        // WHEN
        manager.updateBookmarks(
                DatabaseNameUtil.SYSTEM_DATABASE_NAME, Set.of(initialBookmark2, initialBookmark3), Set.of(newBookmark));
        var bookmarks = manager.getBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME);

        // THEN
        assertEquals(Set.of(initialBookmark0, initialBookmark1, initialBookmark4, newBookmark), bookmarks);
    }

    @Test
    void shouldGetBookmarksFromBookmarkSupplier() {
        // GIVEN
        var initialBookmark = Bookmark.from("SY:000001");
        var initialBookmarks = Map.of(DatabaseNameUtil.SYSTEM_DATABASE_NAME, Set.of(initialBookmark));
        var bookmarkSupplier = mock(BookmarksSupplier.class);
        var supplierBookmark = Bookmark.from("SY:000002");
        given(bookmarkSupplier.getBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME))
                .willReturn(Set.of(supplierBookmark));
        manager = new Neo4jBookmarkManager(initialBookmarks, null, bookmarkSupplier);

        // WHEN
        var bookmarks = manager.getBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME);

        // THEN
        then(bookmarkSupplier).should().getBookmarks(DatabaseNameUtil.SYSTEM_DATABASE_NAME);
        assertEquals(Set.of(initialBookmark, supplierBookmark), bookmarks);
    }

    @Test
    void shouldGetAllBookmarksFromBookmarkSupplier() {
        // GIVEN
        var initialBookmark = Bookmark.from("SY:000001");
        var initialBookmarks = Map.of(DatabaseNameUtil.SYSTEM_DATABASE_NAME, Set.of(initialBookmark));
        var bookmarkSupplier = mock(BookmarksSupplier.class);
        var supplierBookmark = Bookmark.from("SY:000002");
        given(bookmarkSupplier.getAllBookmarks()).willReturn(Set.of(supplierBookmark));
        manager = new Neo4jBookmarkManager(initialBookmarks, null, bookmarkSupplier);

        // WHEN
        var bookmarks = manager.getAllBookmarks();

        // THEN
        then(bookmarkSupplier).should().getAllBookmarks();
        assertEquals(Set.of(initialBookmark, supplierBookmark), bookmarks);
    }
}
