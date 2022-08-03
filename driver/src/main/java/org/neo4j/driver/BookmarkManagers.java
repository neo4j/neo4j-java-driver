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

import org.neo4j.driver.internal.Neo4jBookmarkManager;

/**
 * Setups new instances of {@link BookmarkManager}.
 */
public interface BookmarkManagers {
    /**
     * Setups a new instance of bookmark manager that can be used in {@link org.neo4j.driver.Config.ConfigBuilder#withBookmarkManager(BookmarkManager)}.
     *
     * @param config the bookmark manager configuration
     * @return the bookmark manager
     */
    static BookmarkManager defaultManager(BookmarkManagerConfig config) {
        return new Neo4jBookmarkManager(config.initialBookmarks(), config.updateListener(), config.bookmarkSupplier());
    }
}
