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
package org.neo4j.driver.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.Iterables.asList;

import java.util.HashSet;
import org.hamcrest.Matcher;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;

public class BookmarkUtil {
    /**
     * Bookmark is empty.
     */
    @SuppressWarnings("deprecation")
    public static void assertBookmarkIsEmpty(Bookmark bookmark) {
        assertNotNull(bookmark);
        assertThat(bookmark, instanceOf(InternalBookmark.class));
        assertTrue(bookmark.isEmpty());
    }

    /**
     * Bookmark is not empty but I do not care what value it has.
     */
    @SuppressWarnings("deprecation")
    public static void assertBookmarkIsNotEmpty(Bookmark bookmark) {
        assertNotNull(bookmark);
        assertThat(bookmark, instanceOf(InternalBookmark.class));
        assertFalse(bookmark.isEmpty());
    }

    /**
     * Bookmark contains one single value
     */
    public static void assertBookmarkContainsSingleValue(Bookmark bookmark) {
        assertBookmarkContainsSingleValue(bookmark, notNullValue(String.class));
    }

    /**
     * Bookmark contains one single value and the value matches the requirement set by matcher.
     */
    @SuppressWarnings("deprecation")
    public static void assertBookmarkContainsSingleValue(Bookmark bookmark, Matcher<String> matcher) {
        assertNotNull(bookmark);
        assertThat(bookmark, instanceOf(InternalBookmark.class));

        var values = asList((bookmark).values());
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0), matcher);
    }

    /**
     * Each bookmark contains one single value and the values are all different from each other.
     */
    @SuppressWarnings("deprecation")
    public static void assertBookmarksContainsSingleUniqueValues(Bookmark... bookmarks) {
        var count = bookmarks.length;
        var bookmarkStrings = new HashSet<String>();

        for (var bookmark : bookmarks) {
            assertBookmarkContainsSingleValue(bookmark);
            var values = asList((bookmark).values());
            bookmarkStrings.addAll(values);
        }
        assertEquals(count, bookmarkStrings.size());
    }
}
