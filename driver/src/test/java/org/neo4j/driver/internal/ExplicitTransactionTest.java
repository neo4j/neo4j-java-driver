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
package org.neo4j.driver.internal;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.handlers.BookmarkResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExplicitTransactionTest
{
    @Test
    public void shouldRollbackOnImplicitFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        when( conn.isOpen() ).thenReturn( true );
        SessionResourcesHandler resourcesHandler = mock( SessionResourcesHandler.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, resourcesHandler );

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( ResponseHandler.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "ROLLBACK", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( ResponseHandler.class ) );
        order.verify( conn ).sync();
        verify( resourcesHandler, only() ).onTransactionClosed( tx );
        verifyNoMoreInteractions( conn, resourcesHandler );
    }

    @Test
    public void shouldRollbackOnExplicitFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        when( conn.isOpen() ).thenReturn( true );
        SessionResourcesHandler resourcesHandler = mock( SessionResourcesHandler.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, resourcesHandler );

        // When
        tx.failure();
        tx.success(); // even if success is called after the failure call!
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( BookmarkResponseHandler.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "ROLLBACK", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( BookmarkResponseHandler.class ) );
        order.verify( conn ).sync();
        verify( resourcesHandler, only() ).onTransactionClosed( tx );
        verifyNoMoreInteractions( conn, resourcesHandler );
    }

    @Test
    public void shouldCommitOnSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        when( conn.isOpen() ).thenReturn( true );
        SessionResourcesHandler resourcesHandler = mock( SessionResourcesHandler.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, resourcesHandler );

        // When
        tx.success();
        tx.close();

        // Then

        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( BookmarkResponseHandler.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "COMMIT", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        order.verify( conn ).pullAll( any( BookmarkResponseHandler.class ) );
        order.verify( conn ).sync();
        verify( resourcesHandler, only() ).onTransactionClosed( tx );
        verifyNoMoreInteractions( conn, resourcesHandler );
    }

    @Test
    public void shouldOnlyQueueMessagesWhenNoBookmarkGiven()
    {
        Connection connection = mock( Connection.class );

        new ExplicitTransaction( connection, mock( SessionResourcesHandler.class ), null );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).run( "BEGIN", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        inOrder.verify( connection ).pullAll( NoOpResponseHandler.INSTANCE );
        inOrder.verify( connection, never() ).sync();
    }

    @Test
    public void shouldSyncWhenBookmarkGiven()
    {
        Bookmark bookmark = Bookmark.from( "hi, I'm bookmark" );
        Connection connection = mock( Connection.class );

        new ExplicitTransaction( connection, mock( SessionResourcesHandler.class ), bookmark );

        Map<String,Value> expectedParams = bookmark.asBeginTransactionParameters();

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).run( "BEGIN", expectedParams, NoOpResponseHandler.INSTANCE );
        inOrder.verify( connection ).pullAll( NoOpResponseHandler.INSTANCE );
        inOrder.verify( connection ).sync();
    }

    @Test
    public void shouldBeOpenAfterConstruction()
    {
        Transaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedForSuccess()
    {
        Transaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.success();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedForFailure()
    {
        Transaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.failure();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedToClose()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.markToClose();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterCommit()
    {
        Transaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.success();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterRollback()
    {
        Transaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.failure();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedWhenMarkedToCloseAndClosed()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );

        tx.markToClose();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldHaveEmptyBookmarkInitially()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );
        assertTrue( tx.bookmark().isEmpty() );
    }

    @Test
    public void shouldNotKeepInitialBookmark()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ),
                Bookmark.from( "Dog" ) );
        assertTrue( tx.bookmark().isEmpty() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithNull()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );
        tx.setBookmark( Bookmark.from( "Cat" ) );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
        tx.setBookmark( null );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        ExplicitTransaction tx = new ExplicitTransaction( openConnectionMock(), mock( SessionResourcesHandler.class ) );
        tx.setBookmark( Bookmark.from( "Cat" ) );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
        tx.setBookmark( Bookmark.empty() );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
    }

    private static Connection openConnectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.isOpen() ).thenReturn( true );
        return connection;
    }
}
