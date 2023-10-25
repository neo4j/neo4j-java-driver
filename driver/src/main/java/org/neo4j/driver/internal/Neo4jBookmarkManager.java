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

import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.io.Serial;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;

/**
 * A basic {@link BookmarkManager} implementation.
 */
public final class Neo4jBookmarkManager implements BookmarkManager {
    @Serial
    private static final long serialVersionUID = 6615186840717102303L;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Set<Bookmark> bookmarks;
    private final Consumer<Set<Bookmark>> updateListener;
    private final Supplier<Set<Bookmark>> bookmarksSupplier;

    public Neo4jBookmarkManager(
            Set<Bookmark> initialBookmarks,
            Consumer<Set<Bookmark>> updateListener,
            Supplier<Set<Bookmark>> bookmarksSupplier) {
        Objects.requireNonNull(initialBookmarks, "initialBookmarks must not be null");
        this.bookmarks = new HashSet<>(initialBookmarks);
        this.updateListener = updateListener;
        this.bookmarksSupplier = bookmarksSupplier;
    }

    @Override
    public void updateBookmarks(Set<Bookmark> previousBookmarks, Set<Bookmark> newBookmarks) {
        var immutableBookmarks = executeWithLock(rwLock.writeLock(), () -> {
            this.bookmarks.removeAll(previousBookmarks);
            this.bookmarks.addAll(newBookmarks);
            return Collections.unmodifiableSet(this.bookmarks);
        });
        if (updateListener != null) {
            updateListener.accept(immutableBookmarks);
        }
    }

    @Override
    public Set<Bookmark> getBookmarks() {
        var immutableBookmarks = executeWithLock(rwLock.readLock(), () -> Collections.unmodifiableSet(this.bookmarks));
        if (bookmarksSupplier != null) {
            var bookmarks = new HashSet<>(immutableBookmarks);
            bookmarks.addAll(bookmarksSupplier.get());
            immutableBookmarks = Collections.unmodifiableSet(bookmarks);
        }
        return immutableBookmarks;
    }
}
