/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashSet;

import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void shouldReceiveBookmarkOnSuccessfulCommit() throws Throwable
    {
        // Given
        assertNull( session.lastBookmark() );

        // When
        createNodeInTx( session );

        // Then
        assertNotNull( session.lastBookmark() );
        assertThat( session.lastBookmark(), startsWith( "neo4j:bookmark:v1:tx" ) );
    }

    @Test
    void shouldThrowForInvalidBookmark()
    {
        String invalidBookmark = "hi, this is an invalid bookmark";

        try ( Session session = driver.session( invalidBookmark ) )
        {
            assertThrows( ClientException.class, session::beginTransaction );
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void shouldThrowForUnreachableBookmark()
    {
        createNodeInTx( session );

        TransientException e = assertThrows( TransientException.class, () -> session.beginTransaction( session.lastBookmark() + 42 ) );
        assertThat( e.getMessage(), startsWith( "Database not up to the requested version" ) );
    }

    @Test
    void bookmarkRemainsAfterRolledBackTx()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.failure();
        }

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterTxFailure()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        Transaction tx = session.beginTransaction();
        tx.run( "RETURN" );
        tx.success();

        assertThrows( ClientException.class, tx::close );
        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterSuccessfulSessionRun()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        session.run( "RETURN 1" ).consume();

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkRemainsAfterFailedSessionRun()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        assertThrows( ClientException.class, () -> session.run( "RETURN" ).consume() );
        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    void bookmarkIsUpdatedOnEveryCommittedTx()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );
        String bookmark1 = session.lastBookmark();
        assertNotNull( bookmark1 );

        createNodeInTx( session );
        String bookmark2 = session.lastBookmark();
        assertNotNull( bookmark2 );

        createNodeInTx( session );
        String bookmark3 = session.lastBookmark();
        assertNotNull( bookmark3 );

        assertEquals( 3, new HashSet<>( asList( bookmark1, bookmark2, bookmark3 ) ).size() );
    }

    @Test
    void createSessionWithInitialBookmark()
    {
        String bookmark = "TheBookmark";
        try ( Session session = driver.session( bookmark ) )
        {
            assertEquals( bookmark, session.lastBookmark() );
        }
    }

    @Test
    void createSessionWithAccessModeAndInitialBookmark()
    {
        String bookmark = "TheBookmark";
        try ( Session session = driver.session( AccessMode.WRITE, bookmark ) )
        {
            assertEquals( bookmark, session.lastBookmark() );
        }
    }

    private static void createNodeInTx( Session session )
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.success();
        }
    }
}
