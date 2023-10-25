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

import org.neo4j.driver.internal.Neo4jBookmarkManager;

/**
 * Setups new instances of {@link BookmarkManager}.
 */
public final class BookmarkManagers {
    private BookmarkManagers() {}
    /**
     * Setups a new instance of bookmark manager that can be used in {@link org.neo4j.driver.SessionConfig.Builder#withBookmarkManager(BookmarkManager)}.
     *
     * @param config the bookmark manager configuration
     * @return the bookmark manager
     */
    public static BookmarkManager defaultManager(BookmarkManagerConfig config) {
        return new Neo4jBookmarkManager(
                config.initialBookmarks(),
                config.bookmarksConsumer().orElse(null),
                config.bookmarksSupplier().orElse(null));
    }
}
