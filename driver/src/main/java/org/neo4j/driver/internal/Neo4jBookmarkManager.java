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

import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.io.Serial;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.BookmarkSupplier;

/**
 * A basic {@link BookmarkManager} implementation.
 */
public final class Neo4jBookmarkManager implements BookmarkManager {
    @Serial
    private static final long serialVersionUID = 6615186840717102303L;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Map<String, Set<Bookmark>> databaseToBookmarks = new HashMap<>();
    private final BiConsumer<String, Set<Bookmark>> updateListener;
    private final BookmarkSupplier bookmarkSupplier;

    public Neo4jBookmarkManager(
            Map<String, Set<Bookmark>> initialBookmarks,
            BiConsumer<String, Set<Bookmark>> updateListener,
            BookmarkSupplier bookmarkSupplier) {
        Objects.requireNonNull(initialBookmarks, "initialBookmarks must not be null");
        this.databaseToBookmarks.putAll(initialBookmarks);
        this.updateListener = updateListener;
        this.bookmarkSupplier = bookmarkSupplier;
    }

    @Override
    public void updateBookmarks(String database, Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks) {
        var immutableBookmarks = executeWithLock(
                rwLock.writeLock(),
                () -> databaseToBookmarks.compute(database, (ignored, bookmarks) -> {
                    var updatedBookmarks = new HashSet<Bookmark>();
                    if (bookmarks != null) {
                        bookmarks.stream()
                                .filter(bookmark -> !previousBookmarks.contains(bookmark))
                                .forEach(updatedBookmarks::add);
                    }
                    updatedBookmarks.addAll(newBookmarks);
                    return Collections.unmodifiableSet(updatedBookmarks);
                }));
        if (updateListener != null) {
            updateListener.accept(database, immutableBookmarks);
        }
    }

    @Override
    public Set<Bookmark> getBookmarks(String database) {
        var immutableBookmarks = executeWithLock(
                rwLock.readLock(), () -> databaseToBookmarks.getOrDefault(database, Collections.emptySet()));
        if (bookmarkSupplier != null) {
            var bookmarks = new HashSet<>(immutableBookmarks);
            bookmarks.addAll(bookmarkSupplier.getBookmarks(database));
            immutableBookmarks = Collections.unmodifiableSet(bookmarks);
        }
        return immutableBookmarks;
    }

    @Override
    public Set<Bookmark> getAllBookmarks() {
        var immutableBookmarks = executeWithLock(rwLock.readLock(), () -> databaseToBookmarks.values().stream()
                        .flatMap(Collection::stream))
                .collect(Collectors.toUnmodifiableSet());
        if (bookmarkSupplier != null) {
            var bookmarks = new HashSet<>(immutableBookmarks);
            bookmarks.addAll(bookmarkSupplier.getAllBookmarks());
            immutableBookmarks = Collections.unmodifiableSet(bookmarks);
        }
        return immutableBookmarks;
    }

    @Override
    public void forget(Set<String> databases) {
        executeWithLock(rwLock.writeLock(), () -> databases.forEach(databaseToBookmarks::remove));
    }
}
