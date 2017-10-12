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

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.util.Futures.getBlocking;
import static org.neo4j.driver.v1.util.TestUtil.connectionMock;

public class ExplicitTransactionTest
{
    @Test
    public void shouldRollbackOnImplicitFailure()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        tx.close();

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).run( eq( "BEGIN" ), any(), any(), any() );
        order.verify( connection ).runAndFlush( eq( "ROLLBACK" ), any(), any(), any() );
        order.verify( connection ).releaseInBackground();
    }

    @Test
    public void shouldRollbackOnExplicitFailure()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        tx.failure();
        tx.success(); // even if success is called after the failure call!
        tx.close();

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).run( eq( "BEGIN" ), any(), any(), any() );
        order.verify( connection ).runAndFlush( eq( "ROLLBACK" ), any(), any(), any() );
        order.verify( connection ).releaseInBackground();
    }

    @Test
    public void shouldCommitOnSuccess()
    {
        // Given
        Connection connection = connectionMock();
        ExplicitTransaction tx = beginTx( connection );

        // When
        tx.success();
        tx.close();

        // Then
        InOrder order = inOrder( connection );
        order.verify( connection ).run( eq( "BEGIN" ), any(), any(), any() );
        order.verify( connection ).runAndFlush( eq( "COMMIT" ), any(), any(), any() );
        order.verify( connection ).releaseInBackground();
    }

    @Test
    public void shouldOnlyQueueMessagesWhenNoBookmarkGiven()
    {
        Connection connection = connectionMock();

        beginTx( connection, Bookmark.empty() );

        verify( connection ).run( eq( "BEGIN" ), any(), any(), any() );
        verify( connection, never() ).runAndFlush( any(), any(), any(), any() );
    }

    @Test
    public void shouldFlushWhenBookmarkGiven()
    {
        Bookmark bookmark = Bookmark.from( "hi, I'm bookmark" );
        Connection connection = connectionMock();

        beginTx( connection, bookmark );

        verify( connection ).runAndFlush( eq( "BEGIN" ), any(), any(), any() );
        verify( connection, never() ).run( any(), any(), any(), any() );
    }

    @Test
    public void shouldBeOpenAfterConstruction()
    {
        Transaction tx = beginTx( connectionMock() );

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedForSuccess()
    {
        Transaction tx = beginTx( connectionMock() );

        tx.success();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedForFailure()
    {
        Transaction tx = beginTx( connectionMock() );

        tx.failure();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenWhenMarkedToClose()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.markToClose();

        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterCommit()
    {
        Transaction tx = beginTx( connectionMock() );

        tx.success();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterRollback()
    {
        Transaction tx = beginTx( connectionMock() );

        tx.failure();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedWhenMarkedToCloseAndClosed()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );

        tx.markToClose();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldHaveEmptyBookmarkInitially()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );
        assertTrue( tx.bookmark().isEmpty() );
    }

    @Test
    public void shouldNotKeepInitialBookmark()
    {
        ExplicitTransaction tx = beginTx( connectionMock(), Bookmark.from( "Dog" ) );
        assertTrue( tx.bookmark().isEmpty() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithNull()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );
        tx.setBookmark( Bookmark.from( "Cat" ) );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
        tx.setBookmark( null );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
    }

    @Test
    public void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        ExplicitTransaction tx = beginTx( connectionMock() );
        tx.setBookmark( Bookmark.from( "Cat" ) );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
        tx.setBookmark( Bookmark.empty() );
        assertEquals( "Cat", tx.bookmark().maxBookmarkAsString() );
    }

    private static ExplicitTransaction beginTx( Connection connection )
    {
        return beginTx( connection, Bookmark.empty() );
    }

    private static ExplicitTransaction beginTx( Connection connection, Bookmark initialBookmark )
    {
        return beginTx( connection, mock( NetworkSession.class ), initialBookmark );
    }

    private static ExplicitTransaction beginTx( Connection connection, NetworkSession session,
            Bookmark initialBookmark )
    {
        ExplicitTransaction tx = new ExplicitTransaction( connection, session );
        return getBlocking( tx.beginAsync( initialBookmark ) );
    }
}
