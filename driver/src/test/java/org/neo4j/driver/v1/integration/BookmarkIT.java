/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;

public class BookmarkIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestNeo4jSession sessionRule = new TestNeo4jSession();

    private Driver driver;
    private Session session;

    @Before
    public void assumeBookmarkSupport()
    {
        driver = sessionRule.driver();
        session = sessionRule;

        ServerVersion serverVersion = ServerVersion.version( driver );
        assumeTrue( "Server version `" + serverVersion + "` does not support bookmark",
                serverVersion.greaterThanOrEqual( v3_1_0 ) );
    }

    @Test
    public void shouldConnectIPv6Uri()
    {
        // Given
        try(Driver driver =  GraphDatabase.driver( "bolt://[::1]:7687", DEFAULT_AUTH_TOKEN );
            Session session = driver.session() )
        {
            // When
            StatementResult result = session.run( "RETURN 1" );

            // Then
            assertThat( result.single().get( 0 ).asInt(), equalTo( 1 ) );
        }
    }

    @Test
    public void shouldReceiveBookmarkOnSuccessfulCommit() throws Throwable
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
    public void shouldThrowForInvalidBookmark()
    {
        String invalidBookmark = "hi, this is an invalid bookmark";

        try (Session session = driver.session( invalidBookmark )) {
            try
            {
                session.beginTransaction();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
            }
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void shouldThrowForUnreachableBookmark()
    {
        createNodeInTx( session );

        try
        {
            session.beginTransaction( session.lastBookmark() + 42 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransientException.class ) );
            assertThat( e.getMessage(), startsWith( "Database not up to the requested version" ) );
        }
    }

    @Test
    public void bookmarkRemainsAfterRolledBackTx()
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
    public void bookmarkRemainsAfterTxFailure()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        Transaction tx = session.beginTransaction();
        tx.run( "RETURN" );
        tx.success();
        try
        {
            tx.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    public void bookmarkRemainsAfterSuccessfulSessionRun()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        session.run( "RETURN 1" ).consume();

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    public void bookmarkRemainsAfterFailedSessionRun()
    {
        assertNull( session.lastBookmark() );

        createNodeInTx( session );

        String bookmark = session.lastBookmark();
        assertNotNull( bookmark );

        try
        {
            session.run( "RETURN" ).consume();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        assertEquals( bookmark, session.lastBookmark() );
    }

    @Test
    public void bookmarkIsUpdatedOnEveryCommittedTx()
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
    public void createSessionWithInitialBookmark()
    {
        String bookmark = "TheBookmark";
        try ( Session session = driver.session( bookmark ) )
        {
            assertEquals( bookmark, session.lastBookmark() );
        }
    }

    @Test
    public void createSessionWithAccessModeAndInitialBookmark()
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
