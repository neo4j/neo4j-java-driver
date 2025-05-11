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

import java.io.IOException;
import java.io.ObjectInputStream;
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
    private static final long serialVersionUID = -737795053416084953L;

    transient ReadWriteLock rwLock = new ReentrantReadWriteLock();

    @SuppressWarnings("serial")
    final Set<Bookmark> bookmarks;

    @SuppressWarnings("serial")
    final Consumer<Set<Bookmark>> updateListener;

    @SuppressWarnings("serial")
    final Supplier<Set<Bookmark>> bookmarksSupplier;

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
            return Collections.unmodifiableSet(new HashSet<>(this.bookmarks));
        });
        if (updateListener != null) {
            updateListener.accept(immutableBookmarks);
        }
    }

    @Override
    public Set<Bookmark> getBookmarks() {
        var bookmarks = executeWithLock(rwLock.readLock(), () -> new HashSet<>(this.bookmarks));
        if (bookmarksSupplier != null) {
            bookmarks.addAll(bookmarksSupplier.get());
        }
        return Collections.unmodifiableSet(bookmarks);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwLock = new ReentrantReadWriteLock();
    }
}
