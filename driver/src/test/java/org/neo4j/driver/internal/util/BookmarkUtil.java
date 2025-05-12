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
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hamcrest.Matcher;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;

public class BookmarkUtil {
    /**
     * Bookmark contains the value that matches the requirement set by matcher.
     */
    public static void assertBookmarkContainsValue(Bookmark bookmark, Matcher<String> matcher) {
        assertNotNull(bookmark);
        assertThat(bookmark, instanceOf(InternalBookmark.class));

        assertThat(bookmark.value(), matcher);
    }
}
