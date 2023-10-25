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

import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarkContainsSingleValue;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarkIsEmpty;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarksContainsSingleUniqueValues;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;

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
    @SuppressWarnings("deprecation")
    void shouldReceiveBookmarkOnSuccessfulCommit() {
        // Given
        assertBookmarkIsEmpty(session.lastBookmark());

        // When
        createNodeInTx(session);

        // Then
        assertBookmarkContainsSingleValue(session.lastBookmark(), startsWith("neo4j:bookmark:v1:tx"));
    }

    @Test
    @EnabledOnNeo4jWith(Neo4jFeature.BOLT_V4)
    @SuppressWarnings("deprecation")
    void shouldReceiveNewBookmarkOnSuccessfulCommit() {
        // Given
        var initialBookmark = session.lastBookmark();
        assertBookmarkIsEmpty(initialBookmark);

        // When
        createNodeInTx(session);

        // Then
        assertBookmarkContainsSingleValue(session.lastBookmark());
        assertNotEquals(initialBookmark, session.lastBookmark());
    }

    @Test
    void shouldThrowForInvalidBookmark() {
        var invalidBookmark = parse("hi, this is an invalid bookmark");

        try (var session =
                driver.session(builder().withBookmarks(invalidBookmark).build())) {
            assertThrows(ClientException.class, session::beginTransaction);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void bookmarkRemainsAfterRolledBackTx() {
        assertBookmarkIsEmpty(session.lastBookmark());

        createNodeInTx(session);

        var bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark);

        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (a:Person)");
            tx.rollback();
        }

        assertEquals(bookmark, session.lastBookmark());
    }

    @Test
    @SuppressWarnings("deprecation")
    void bookmarkRemainsAfterTxFailure() {
        assertBookmarkIsEmpty(session.lastBookmark());

        createNodeInTx(session);

        var bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark);

        var tx = session.beginTransaction();

        assertThrows(ClientException.class, () -> tx.run("RETURN"));
        assertEquals(bookmark, session.lastBookmark());
    }

    @Test
    @SuppressWarnings("deprecation")
    void bookmarkRemainsAfterSuccessfulSessionRun() {
        assertBookmarkIsEmpty(session.lastBookmark());

        createNodeInTx(session);

        var bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark);

        session.run("RETURN 1").consume();

        assertEquals(bookmark, session.lastBookmark());
    }

    @Test
    @SuppressWarnings("deprecation")
    void bookmarkRemainsAfterFailedSessionRun() {
        assertBookmarkIsEmpty(session.lastBookmark());

        createNodeInTx(session);

        var bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark);

        assertThrows(ClientException.class, () -> session.run("RETURN").consume());
        assertEquals(bookmark, session.lastBookmark());
    }

    @Test
    @SuppressWarnings("deprecation")
    void bookmarkIsUpdatedOnEveryCommittedTx() {
        assertBookmarkIsEmpty(session.lastBookmark());

        createNodeInTx(session);
        var bookmark1 = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark1);

        createNodeInTx(session);
        var bookmark2 = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark2);

        createNodeInTx(session);
        var bookmark3 = session.lastBookmark();
        assertBookmarkContainsSingleValue(bookmark3);

        assertBookmarksContainsSingleUniqueValues(bookmark1, bookmark2, bookmark3);
    }

    @Test
    @SuppressWarnings("deprecation")
    void createSessionWithInitialBookmark() {
        var bookmark = parse("TheBookmark");
        try (var session = driver.session(builder().withBookmarks(bookmark).build())) {
            assertEquals(bookmark, session.lastBookmark());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void createSessionWithAccessModeAndInitialBookmark() {
        var bookmark = parse("TheBookmark");
        try (var session = driver.session(builder().withBookmarks(bookmark).build())) {
            assertEquals(bookmark, session.lastBookmark());
        }
    }

    private static void createNodeInTx(Session session) {
        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (a:Person)");
            tx.commit();
        }
    }
}
