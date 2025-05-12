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

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Bookmark configuration used to configure bookmark manager supplied by {@link BookmarkManagers#defaultManager(BookmarkManagerConfig)}.
 */
public final class BookmarkManagerConfig {
    private final Set<Bookmark> initialBookmarks;
    private final Consumer<Set<Bookmark>> bookmarksConsumer;
    private final Supplier<Set<Bookmark>> bookmarksSupplier;

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
     * @return the set of bookmarks
     */
    public Set<Bookmark> initialBookmarks() {
        return initialBookmarks;
    }

    /**
     * Returns bookmarks consumer that will be notified when bookmarks are updated.
     *
     * @return the bookmarks consumer
     */
    public Optional<Consumer<Set<Bookmark>>> bookmarksConsumer() {
        return Optional.ofNullable(bookmarksConsumer);
    }

    /**
     * Returns bookmarks supplier that will be used by the bookmark manager when getting bookmarks.
     *
     * @return the bookmark supplier
     */
    public Optional<Supplier<Set<Bookmark>>> bookmarksSupplier() {
        return Optional.ofNullable(bookmarksSupplier);
    }

    /**
     * Builder used to configure {@link BookmarkManagerConfig} which will be used to create a bookmark manager.
     */
    public static final class BookmarkManagerConfigBuilder {
        private Set<Bookmark> initialBookmarks = Collections.emptySet();
        private Consumer<Set<Bookmark>> bookmarksConsumer;
        private Supplier<Set<Bookmark>> bookmarksSupplier;

        private BookmarkManagerConfigBuilder() {}

        /**
         * Provide a map of initial bookmarks to initialise the bookmark manager.
         *
         * @param initialBookmarks initial set of bookmarks
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withInitialBookmarks(Set<Bookmark> initialBookmarks) {
            Objects.requireNonNull(initialBookmarks, "initialBookmarks must not be null");
            this.initialBookmarks = initialBookmarks;
            return this;
        }

        /**
         * Provide bookmarks consumer.
         * <p>
         * The consumer will be called outside bookmark manager's synchronisation lock.
         * <p>
         * If serialization of {@link BookmarkManager} is required, the instance supplied MUST implement {@link java.io.Serializable}.
         *
         * @param bookmarksConsumer bookmarks consumer
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withBookmarksConsumer(Consumer<Set<Bookmark>> bookmarksConsumer) {
            this.bookmarksConsumer = bookmarksConsumer;
            return this;
        }

        /**
         * Provide bookmarks supplier.
         * <p>
         * The supplied bookmarks will be served alongside the bookmarks served by the bookmark manager. The supplied bookmarks will not be stored nor managed
         * by the bookmark manager.
         * <p>
         * The supplier will be called outside bookmark manager's synchronisation lock.
         * <p>
         * If serialization of {@link BookmarkManager} is required, the instance supplied MUST implement {@link java.io.Serializable}.
         *
         * @param bookmarksSupplier the bookmarks supplier
         * @return this builder
         */
        public BookmarkManagerConfigBuilder withBookmarksSupplier(Supplier<Set<Bookmark>> bookmarksSupplier) {
            this.bookmarksSupplier = bookmarksSupplier;
            return this;
        }

        /**
         * Builds an instance of {@link BookmarkManagerConfig}.
         * @return the config
         */
        public BookmarkManagerConfig build() {
            return new BookmarkManagerConfig(this);
        }
    }
}
