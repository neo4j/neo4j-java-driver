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
package org.neo4j.driver.integration;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarkContainsSingleValue;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarkIsEmpty;
import static org.neo4j.driver.internal.util.BookmarkUtil.assertBookmarksContainsSingleUniqueValues;

@ParallelizableIT
class BookmarkIT
{
    @RegisterExtension
    static final SessionExtension sessionRule = new SessionExtension();

    private Driver driver;
    private Session session;

    @BeforeEach
    void assumeBookmarkSupport()
    {
        driver = sessionRule.driver();
        session = sessionRule;
    }

    @Test
    @DisabledOnNeo4jWith( Neo4jFeature.BOLT_V4 )
    void shouldReceiveBookmarkOnSuccessfulCommit() throws Throwable
    {
        // Given
        assertBookmarkIsEmpty( session.lastBookmark() );

        // When
        createNodeInTx( session );

        // Then
        assertBookmarkContainsSingleValue( session.lastBookmark(), startsWith( "neo4j:bookmark:v1:tx" ) );
    }

    @Test
    @EnabledOnNeo4jWith( Neo4jFeature.BOLT_V4 )
    void shouldReceiveNewBookmarkOnSuccessfulCommit() throws Throwable
    {
        // Given
        Bookmark initialBookmark = session.lastBookmark();
        assertBookmarkIsEmpty( initialBookmark );

        // When
        createNodeInTx( session );

        // Then
        assertBookmarkContainsSingleValue( session.lastBookmark() );
        assertNotEquals( initialBookmark, session.lastBookmark() );
    }

    @Test
    void shouldThrowForInvalidBookmark()
    {
        Bookmark invalidBookmark = parse( "hi, this is an invalid bookmark" );

        try ( Session session = driver.session( builder().withBookmarks( invalidBookmark ).build() ) )
        {
            assertThrows( ClientException.class, session::beginTransaction );
        }
    }

    @Test
    void bookmarkRemainsAfterRolledBackTx()
    {
        assertBookmarkIsEmpty( session.lastBookmark() );

        createNodeInTx( session );

        Bookmark bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark );

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.rollback();
        }

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterTxFailure()
    {
        assertBookmarkIsEmpty( session.lastBookmark() );

        createNodeInTx( session );

        Bookmark bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark );

        Transaction tx = session.beginTransaction();
        tx.run( "RETURN" );

        assertThrows( ClientException.class, tx::commit );
        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterSuccessfulSessionRun()
    {
        assertBookmarkIsEmpty( session.lastBookmark() );

        createNodeInTx( session );

        Bookmark bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark );

        session.run( "RETURN 1" ).consume();

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterFailedSessionRun()
    {
        assertBookmarkIsEmpty( session.lastBookmark() );

        createNodeInTx( session );

        Bookmark bookmark = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark );

        assertThrows( ClientException.class, () -> session.run( "RETURN" ).consume() );
        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkIsUpdatedOnEveryCommittedTx()
    {
        assertBookmarkIsEmpty( session.lastBookmark() );

        createNodeInTx( session );
        Bookmark bookmark1 = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark1 );

        createNodeInTx( session );
        Bookmark bookmark2 = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark2 );

        createNodeInTx( session );
        Bookmark bookmark3 = session.lastBookmark();
        assertBookmarkContainsSingleValue( bookmark3 );

        assertBookmarksContainsSingleUniqueValues( bookmark1, bookmark2, bookmark3 );
    }

    @Test
    void createSessionWithInitialBookmark()
    {
        Bookmark bookmark = parse( "TheBookmark" );
        try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() ) )
        {
            assertEquals( bookmark, session.lastBookmark() );
        }
    }

    @Test
    void createSessionWithAccessModeAndInitialBookmark()
    {
        Bookmark bookmark = parse( "TheBookmark" );
        try ( Session session = driver.session( builder().withBookmarks( bookmark ).build() ) )
        {
            assertEquals( bookmark, session.lastBookmark() );
        }
    }

    private static void createNodeInTx( Session session )
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.commit();
        }
    }

    private static boolean isUuid( String string )
    {
        try
        {
            UUID.fromString( string );
        }
        catch ( IllegalArgumentException | NullPointerException e )
        {
            return false;
        }
        return true;
    }

    private static boolean isNumeric( String string )
    {
        try
        {
            Long.parseLong( string );
        }
        catch ( NumberFormatException | NullPointerException e )
        {
            return false;
        }
        return true;
    }
}
