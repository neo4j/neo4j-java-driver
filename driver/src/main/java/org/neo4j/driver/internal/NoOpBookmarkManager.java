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

import java.io.Serial;
import java.util.Collections;
import java.util.Set;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;

/**
 * A no-op {@link BookmarkManager} implementation.
 */
public class NoOpBookmarkManager implements BookmarkManager {
    @Serial
    private static final long serialVersionUID = 7175136719562680362L;

    public static final NoOpBookmarkManager INSTANCE = new NoOpBookmarkManager();

    private static final Set<Bookmark> EMPTY = Collections.emptySet();

    private NoOpBookmarkManager() {}

    @Override
    public void updateBookmarks(Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks) {
        // ignored
    }

    @Override
    public Set<Bookmark> getBookmarks() {
        return EMPTY;
    }
}
