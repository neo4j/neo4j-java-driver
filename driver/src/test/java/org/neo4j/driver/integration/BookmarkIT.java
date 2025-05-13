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
package org.neo4j.driver.integration;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarkContainsValue;

import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;
import org.testcontainers.shaded.com.google.common.collect.Streams;

@ParallelizableIT
class BookmarkIT {
    @RegisterExtension
    static final SessionExtension sessionRule = new SessionExtension();

    private Driver driver;
    private Session session;

    @BeforeEach
    void assumeBookmarkSupport() {
        driver = sessionRule.driver();
        session = sessionRule;
    }

    @Test
    @DisabledOnNeo4jWith(Neo4jFeature.BOLT_V4)
    void shouldReceiveBookmarkOnSuccessfulCommit() {
        // Given
        assertTrue(session.lastBookmarks().isEmpty());

        // When
        createNodeInTx(session);

        // Then
        assertEquals(1, session.lastBookmarks().size());
        assertBookmarkContainsValue(session.lastBookmarks().iterator().next(), startsWith("neo4j:bookmark:v1:tx"));
    }

    @Test
    @EnabledOnNeo4jWith(Neo4jFeature.BOLT_V4)
    void shouldReceiveNewBookmarkOnSuccessfulCommit() {
        // Given
        assertTrue(session.lastBookmarks().isEmpty());

        // When
        createNodeInTx(session);

        // Then
        assertEquals(1, session.lastBookmarks().size());
    }

    @Test
    void shouldThrowForInvalidBookmark() {
        var invalidBookmark = Bookmark.from("hi, this is an invalid bookmark");

        try (var session =
                driver.session(builder().withBookmarks(invalidBookmark).build())) {
            assertThrows(ClientException.class, session::beginTransaction);
        }
    }

    @Test
    void bookmarkRemainsAfterRolledBackTx() {
        assertTrue(session.lastBookmarks().isEmpty());

        createNodeInTx(session);

        var bookmarks = session.lastBookmarks();
        assertFalse(bookmarks.isEmpty());

        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (a:Person)");
            tx.rollback();
        }

        assertEquals(bookmarks, session.lastBookmarks());
    }

    @Test
    void bookmarkRemainsAfterTxFailure() {
        assertTrue(session.lastBookmarks().isEmpty());

        createNodeInTx(session);

        var bookmarks = session.lastBookmarks();
        assertFalse(bookmarks.isEmpty());

        var tx = session.beginTransaction();

        assertThrows(ClientException.class, () -> tx.run("RETURN"));
        assertEquals(bookmarks, session.lastBookmarks());
    }

    @Test
    void bookmarkRemainsAfterSuccessfulSessionRun() {
        assertTrue(session.lastBookmarks().isEmpty());

        createNodeInTx(session);

        var bookmarks = session.lastBookmarks();
        assertFalse(bookmarks.isEmpty());

        session.run("RETURN 1").consume();

        assertEquals(bookmarks, session.lastBookmarks());
    }

    @Test
    void bookmarkRemainsAfterFailedSessionRun() {
        assertTrue(session.lastBookmarks().isEmpty());

        createNodeInTx(session);

        var bookmarks = session.lastBookmarks();
        assertFalse(bookmarks.isEmpty());

        assertThrows(ClientException.class, () -> session.run("RETURN").consume());
        assertEquals(bookmarks, session.lastBookmarks());
    }

    @Test
    void bookmarkIsUpdatedOnEveryCommittedTx() {
        assertTrue(session.lastBookmarks().isEmpty());

        createNodeInTx(session);
        var bookmarks1 = session.lastBookmarks();
        assertEquals(1, bookmarks1.size());

        createNodeInTx(session);
        var bookmarks2 = session.lastBookmarks();
        assertEquals(1, bookmarks2.size());

        createNodeInTx(session);
        var bookmarks3 = session.lastBookmarks();
        assertEquals(1, bookmarks3.size());

        var uniqueNumber = Streams.concat(bookmarks1.stream(), bookmarks2.stream(), bookmarks3.stream())
                .map(Bookmark::value)
                .collect(Collectors.toSet())
                .size();
        assertEquals(3, uniqueNumber);
    }

    @Test
    void createSessionWithInitialBookmark() {
        var bookmark = Bookmark.from("TheBookmark");
        try (var session = driver.session(builder().withBookmarks(bookmark).build())) {
            assertEquals(bookmark, session.lastBookmarks().iterator().next());
        }
    }

    @Test
    void createSessionWithAccessModeAndInitialBookmark() {
        var bookmark = Bookmark.from("TheBookmark");
        try (var session = driver.session(builder().withBookmarks(bookmark).build())) {
            assertEquals(bookmark, session.lastBookmarks().iterator().next());
        }
    }

    private static void createNodeInTx(Session session) {
        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (a:Person)");
            tx.commit();
        }
    }
}
