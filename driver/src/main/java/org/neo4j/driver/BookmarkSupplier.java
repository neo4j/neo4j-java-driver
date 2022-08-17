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

import java.util.Set;

/**
 * Supplies additional bookmarks to {@link BookmarkManager} implementation provided by {@link BookmarkManagers#defaultManager(BookmarkManagerConfig)}.
 * <p>
 * Implementations must avoid calling driver.
 */
public interface BookmarkSupplier {
    /**
     * Supplies a set of bookmarks for a given database.
     *
     * @param database the database name, must not be {@code null}
     * @return the set of bookmarks, must not be {@code null}
     */
    Set<Bookmark> getBookmarks(String database);

    /**
     * Supplies a set of bookmarks for all databases.
     *
     * @return the set of bookmarks, must not be {@code null}
     */
    Set<Bookmark> getAllBookmarks();
}
