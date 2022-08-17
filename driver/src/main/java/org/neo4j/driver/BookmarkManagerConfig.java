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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Bookmark configuration used to configure bookmark manager supplied by {@link BookmarkManagers#defaultManager(BookmarkManagerConfig)}.
 */
public final class BookmarkManagerConfig {
    private final Map<String, Set<Bookmark>> initialBookmarks;
    private final BiConsumer<String, Set<Bookmark>> bookmarksConsumer;
    private final BookmarksSupplier bookmarksSupplier;

    private BookmarkManagerConfig(BookmarkManagerConfigBuilder builder) {
        this.initialBookmarks = builder.initialBookmarks;
        this.bookmarksConsumer = builder.bookmarksConsumer;
        this.bookmarksSupplier = builder.bookmarksSupplier;
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
     * Returns bookmarks consumer that will be notified when database bookmarks are updated.
     *
     * @return the bookmarks consumer
     */
    public Optional<BiConsumer<String, Set<Bookmark>>> bookmarksConsumer() {
        return Optional.ofNullable(bookmarksConsumer);
    }

    /**
     * Returns bookmarks supplier that will be used by the bookmark manager when getting bookmarks.
     *
     * @return the bookmark supplier
     */
    public Optional<BookmarksSupplier> bookmarksSupplier() {
        return Optional.ofNullable(bookmarksSupplier);
    }

    /**
     * Builder used to configure {@link BookmarkManagerConfig} which will be used to create a bookmark manager.
     */
    public static class BookmarkManagerConfigBuilder {
        private Map<String, Set<Bookmark>> initialBookmarks = Collections.emptyMap();
        private BiConsumer<String, Set<Bookmark>> bookmarksConsumer;
        private BookmarksSupplier bookmarksSupplier;

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
         * Provide bookmarks consumer.
         * <p>
         * The consumer will be called outside bookmark manager's synchronisation lock.
         *
         * @param bookmarksConsumer bookmarks consumer
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withBookmarksConsumer(BiConsumer<String, Set<Bookmark>> bookmarksConsumer) {
            this.bookmarksConsumer = bookmarksConsumer;
            return this;
        }

        /**
         * Provide bookmarks supplier.
         * <p>
         * The supplied bookmarks will be served alongside the bookmarks served by the bookmark manager. The supplied bookmarks will not be stored nor managed by the bookmark manager.
         * <p>
         * The supplier will be called outside bookmark manager's synchronisation lock.
         *
         * @param bookmarksSupplier the bookmarks supplier
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withBookmarksSupplier(BookmarksSupplier bookmarksSupplier) {
            this.bookmarksSupplier = bookmarksSupplier;
            return this;
        }

        public BookmarkManagerConfig build() {
            return new BookmarkManagerConfig(this);
        }
    }
}
