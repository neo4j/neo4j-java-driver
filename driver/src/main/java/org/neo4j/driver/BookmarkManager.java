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

import java.io.Serializable;
import java.util.Set;

/**
 * Keeps track of bookmarks and is used by the driver to ensure causal consistency between sessions and query executions.
 * <p>
 * Please note that implementations of this interface MUST NOT block for extended periods of time.
 * <p>
 * Implementations must avoid calling driver.
 *
 * @see org.neo4j.driver.SessionConfig.Builder#withBookmarkManager(BookmarkManager)
 */
public interface BookmarkManager extends Serializable {
    /**
     * Updates bookmarks by deleting the given previous bookmarks and adding the new bookmarks.
     *
     * @param previousBookmarks the previous bookmarks
     * @param newBookmarks      the new bookmarks
     */
    void updateBookmarks(Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks);

    /**
     * Gets an immutable set of bookmarks.
     *
     * @return the set of bookmarks.
     */
    Set<Bookmark> getBookmarks();
}
