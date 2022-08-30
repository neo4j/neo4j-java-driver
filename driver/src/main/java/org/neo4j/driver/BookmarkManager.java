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

import java.io.Serializable;
import java.util.Set;
import org.neo4j.driver.util.Experimental;

/**
 * Keeps track of database bookmarks and is used by the driver to ensure causal consistency between sessions and query executions.
 * <p>
 * Please note that implementations of this interface MUST NOT block for extended periods of time.
 * <p>
 * Implementations must avoid calling driver.
 *
 * @see org.neo4j.driver.SessionConfig.Builder#withBookmarkManager(BookmarkManager)
 */
@Experimental
public interface BookmarkManager extends Serializable {
    /**
     * Updates database bookmarks by deleting the given previous bookmarks and adding the new bookmarks.
     *
     * @param database          the database name, this might be an empty string when session has no database name configured and database discovery is unavailable
     * @param previousBookmarks the previous bookmarks
     * @param newBookmarks      the new bookmarks
     */
    void updateBookmarks(String database, Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks);

    /**
     * Gets an immutable set of bookmarks for a given database.
     *
     * @param database the database name
     * @return the set of bookmarks or an empty set if the database name is unknown to the bookmark manager
     */
    Set<Bookmark> getBookmarks(String database);

    /**
     * Gets an immutable set of bookmarks for all databases.
     *
     * @return the set of bookmarks or an empty set
     */
    Set<Bookmark> getAllBookmarks();

    /**
     * Deletes bookmarks for the given databases.
     * <p>
     * This method should be called by driver users if data deletion is desired when bookmarks for the given databases are no longer needed.
     *
     * @param databases the set of database names
     */
    void forget(Set<String> databases);
}
