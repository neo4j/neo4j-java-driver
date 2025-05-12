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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.io.NotSerializableException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.testutil.TestUtil;

class Neo4jBookmarkManagerTest {
    Neo4jBookmarkManager manager;

    @Test
    void shouldRejectNullInitialBookmarks() {
        assertThrows(NullPointerException.class, () -> new Neo4jBookmarkManager(null, null, null));
    }

    @Test
    void shouldAddInitialBookmarks() {
        // GIVEN
        var initialBookmarks = Set.of(Bookmark.from("SY:000001"));
        manager = new Neo4jBookmarkManager(initialBookmarks, null, null);

        // WHEN & THEN
        assertEquals(initialBookmarks, manager.getBookmarks());
    }

    @Test
    void shouldNotifyUpdateListener() {
        // GIVEN
        @SuppressWarnings("unchecked")
        Consumer<Set<Bookmark>> updateListener = mock(Consumer.class);
        manager = new Neo4jBookmarkManager(Collections.emptySet(), updateListener, null);
        var bookmark = Bookmark.from("SY:000001");

        // WHEN
        manager.updateBookmarks(Collections.emptySet(), Set.of(bookmark));

        // THEN
        then(updateListener).should().accept(Set.of(bookmark));
    }

    @Test
    void shouldUpdateBookmarks() {
        // GIVEN
        var initialBookmark0 = Bookmark.from("SY:000001");
        var initialBookmark1 = Bookmark.from("SY:000002");
        var initialBookmark2 = Bookmark.from("SY:000003");
        var initialBookmark3 = Bookmark.from("SY:000004");
        var initialBookmark4 = Bookmark.from("SY:000005");
        var initialBookmarks =
                Set.of(initialBookmark0, initialBookmark1, initialBookmark2, initialBookmark3, initialBookmark4);
        manager = new Neo4jBookmarkManager(initialBookmarks, null, null);
        var newBookmark = Bookmark.from("SY:000007");

        // WHEN
        manager.updateBookmarks(Set.of(initialBookmark2, initialBookmark3), Set.of(newBookmark));
        var bookmarks = manager.getBookmarks();

        // THEN
        assertEquals(Set.of(initialBookmark0, initialBookmark1, initialBookmark4, newBookmark), bookmarks);
    }

    @Test
    void shouldGetBookmarksFromBookmarkSupplier() {
        // GIVEN
        var initialBookmark = Bookmark.from("SY:000001");
        var initialBookmarks = Set.of(initialBookmark);
        @SuppressWarnings("unchecked")
        Supplier<Set<Bookmark>> bookmarkSupplier = mock(Supplier.class);
        var supplierBookmark = Bookmark.from("SY:000002");
        given(bookmarkSupplier.get()).willReturn(Set.of(supplierBookmark));
        manager = new Neo4jBookmarkManager(initialBookmarks, null, bookmarkSupplier);

        // WHEN
        var bookmarks = manager.getBookmarks();

        // THEN
        then(bookmarkSupplier).should().get();
        assertEquals(Set.of(initialBookmark, supplierBookmark), bookmarks);
    }

    @Test
    void shouldSerialize() throws Exception {
        var manager = new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), null, null);
        var deserializedManager = TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class);

        assertEquals(manager.bookmarks, deserializedManager.bookmarks);
        assertNotNull(deserializedManager.rwLock);
        assertNull(deserializedManager.updateListener);
        assertNull(deserializedManager.bookmarksSupplier);
    }

    @Test
    void shouldSerializeWithUpdateListener() throws Exception {
        var manager = new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), new SerializableUpdateListener(), null);
        var deserializedManager = TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class);

        assertEquals(manager.bookmarks, deserializedManager.bookmarks);
        assertNotNull(deserializedManager.rwLock);
        assertNotNull(deserializedManager.updateListener);
        assertNull(deserializedManager.bookmarksSupplier);
    }

    @Test
    void shouldSerializeWithBookmarksSupplier() throws Exception {
        var manager =
                new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), null, new SerializableBookmarksSupplier());
        var deserializedManager = TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class);

        assertEquals(manager.bookmarks, deserializedManager.bookmarks);
        assertNotNull(deserializedManager.rwLock);
        assertNull(deserializedManager.updateListener);
        assertNotNull(deserializedManager.bookmarksSupplier);
    }

    @Test
    void shouldSerializeWithUpdateListenerAndBookmarksSupplier() throws Exception {
        var manager = new Neo4jBookmarkManager(
                Set.of(Bookmark.from("value")), new SerializableUpdateListener(), new SerializableBookmarksSupplier());
        var deserializedManager = TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class);

        assertEquals(manager.bookmarks, deserializedManager.bookmarks);
        assertNotNull(deserializedManager.rwLock);
        assertNotNull(deserializedManager.updateListener);
        assertNotNull(deserializedManager.bookmarksSupplier);
    }

    @Test
    void shouldNotSerializeWithNonSerializableUpdateListener() {
        var manager = new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), bookmarks -> {}, null);
        assertThrows(
                NotSerializableException.class,
                () -> TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class));
    }

    @Test
    void shouldNotSerializeWithNonSerializableBookmarksSupplier() {
        var manager = new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), null, () -> null);
        assertThrows(
                NotSerializableException.class,
                () -> TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class));
    }

    @Test
    void shouldNotSerializeWithNonSerializableUpdateListenerAndNonSerializableBookmarksSupplier() {
        var manager = new Neo4jBookmarkManager(Set.of(Bookmark.from("value")), bookmarks -> {}, () -> null);
        assertThrows(
                NotSerializableException.class,
                () -> TestUtil.serializeAndReadBack(manager, Neo4jBookmarkManager.class));
    }

    private static class SerializableUpdateListener implements Consumer<Set<Bookmark>>, Serializable {
        @Serial
        private static final long serialVersionUID = -6678581541881918735L;

        @Override
        public void accept(Set<Bookmark> bookmarks) {}
    }

    private static class SerializableBookmarksSupplier implements Supplier<Set<Bookmark>>, Serializable {
        @Serial
        private static final long serialVersionUID = -3978173492063939230L;

        @Override
        public Set<Bookmark> get() {
            return Set.of();
        }
    }
}
