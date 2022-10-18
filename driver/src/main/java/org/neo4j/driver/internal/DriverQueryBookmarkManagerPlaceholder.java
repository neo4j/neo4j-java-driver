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

import java.io.Serial;
import java.util.Set;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;

public class DriverQueryBookmarkManagerPlaceholder implements BookmarkManager {
    @Serial
    private static final long serialVersionUID = 1075016673435512326L;

    public static final DriverQueryBookmarkManagerPlaceholder INSTANCE = new DriverQueryBookmarkManagerPlaceholder();

    private DriverQueryBookmarkManagerPlaceholder() {}

    @Override
    public void updateBookmarks(String database, Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks) {
        throw new UnsupportedOperationException("operation not supported");
    }

    @Override
    public Set<Bookmark> getBookmarks(String database) {
        throw new UnsupportedOperationException("operation not supported");
    }

    @Override
    public Set<Bookmark> getAllBookmarks() {
        throw new UnsupportedOperationException("operation not supported");
    }

    @Override
    public void forget(Set<String> databases) {
        throw new UnsupportedOperationException("operation not supported");
    }
}
