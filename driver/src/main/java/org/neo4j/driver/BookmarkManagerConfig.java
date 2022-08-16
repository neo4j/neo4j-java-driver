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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Bookmark configuration used to configure bookmark manager supplied by {@link BookmarkManagers#defaultManager(BookmarkManagerConfig)}.
 */
public final class BookmarkManagerConfig {
    private final Map<String, Set<Bookmark>> initialBookmarks;
    private final BiConsumer<String, Set<Bookmark>> updateListener;
    private final BookmarkSupplier bookmarkSupplier;

    private BookmarkManagerConfig(BookmarkManagerConfigBuilder builder) {
        this.initialBookmarks = builder.initialBookmarks;
        this.updateListener = builder.updateListener;
        this.bookmarkSupplier = builder.bookmarkSupplier;
    }

    /**
     * Creates a new {@link BookmarkManagerConfigBuilder} used to construct a configuration object.
     *
     * @return a bookmark manager configuration builder.
     */
    public static BookmarkManagerConfigBuilder builder() {
        return new BookmarkManagerConfigBuilder();
    }

    /**
     * Returns the map of bookmarks used to initialise the bookmark manager.
     *
     * @return the map of bookmarks
     */
    public Map<String, Set<Bookmark>> initialBookmarks() {
        return initialBookmarks;
    }

    /**
     * Returns a bookmark update listener that will be notified when database bookmarks are updated.
     *
     * @return the update listener or {@code null}
     */
    public BiConsumer<String, Set<Bookmark>> updateListener() {
        return updateListener;
    }

    /**
     * Returns bookmark supplier that will be used by the bookmark manager when getting bookmarks.
     *
     * @return the bookmark supplier or {@code null}
     */
    public BookmarkSupplier bookmarkSupplier() {
        return bookmarkSupplier;
    }

    /**
     * Builder used to configure {@link BookmarkManagerConfig} which will be used to create a bookmark manager.
     */
    public static class BookmarkManagerConfigBuilder {
        private Map<String, Set<Bookmark>> initialBookmarks = Collections.emptyMap();
        private BiConsumer<String, Set<Bookmark>> updateListener;
        private BookmarkSupplier bookmarkSupplier;

        private BookmarkManagerConfigBuilder() {}

        /**
         * Provide a map of initial bookmarks to initialise the bookmark manager.
         *
         * @param databaseToBookmarks database to bookmarks map
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withInitialBookmarks(Map<String, Set<Bookmark>> databaseToBookmarks) {
            Objects.requireNonNull(databaseToBookmarks, "databaseToBookmarks must not be null");
            this.initialBookmarks = databaseToBookmarks;
            return this;
        }

        /**
         * Provide a bookmarks update listener.
         * <p>
         * The listener will be called outside bookmark manager's synchronisation lock.
         *
         * @param updateListener update listener
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withUpdateListener(BiConsumer<String, Set<Bookmark>> updateListener) {
            this.updateListener = updateListener;
            return this;
        }

        /**
         * Provide a bookmark supplier.
         * <p>
         * The supplied bookmarks will be served alongside the bookmarks served by the bookmark manager. The supplied bookmarks will not be stored nor managed by the bookmark manager.
         * <p>
         * The supplier will be called outside bookmark manager's synchronisation lock.
         *
         * @param bookmarkSupplier the bookmarks supplier
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withBookmarksSupplier(BookmarkSupplier bookmarkSupplier) {
            this.bookmarkSupplier = bookmarkSupplier;
            return this;
        }

        public BookmarkManagerConfig build() {
            return new BookmarkManagerConfig(this);
        }
    }
}
