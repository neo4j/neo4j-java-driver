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

import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Value;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Values.value;

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
        order.verify( conn ).run( "BEGIN", Collections.<String, Value>emptyMap(),Collector.NO_OP );
        order.verify( conn ).pullAll( any( Collector.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "ROLLBACK", Collections.<String, Value>emptyMap(), Collector.NO_OP );
        order.verify( conn ).pullAll( any( Collector.class ) );
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
        order.verify( conn ).run( "BEGIN", Collections.<String, Value>emptyMap(), Collector.NO_OP );
        order.verify( conn ).pullAll( any( BookmarkCollector.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "ROLLBACK", Collections.<String, Value>emptyMap(), Collector.NO_OP );
        order.verify( conn ).pullAll( any( BookmarkCollector.class ) );
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
        order.verify( conn ).run( "BEGIN", Collections.<String, Value>emptyMap(), Collector.NO_OP );
        order.verify( conn ).pullAll( any( BookmarkCollector.class ) );
        order.verify( conn ).isOpen();
        order.verify( conn ).run( "COMMIT", Collections.<String, Value>emptyMap(), Collector.NO_OP );
        order.verify( conn ).pullAll( any( BookmarkCollector.class ) );
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
        inOrder.verify( connection ).run( "BEGIN", Collections.<String,Value>emptyMap(), Collector.NO_OP );
        inOrder.verify( connection ).pullAll( Collector.NO_OP );
        inOrder.verify( connection, never() ).sync();
    }

    @Test
    public void shouldSyncWhenBookmarkGiven()
    {
        String bookmark = "hi, I'm bookmark";
        Connection connection = mock( Connection.class );

        new ExplicitTransaction( connection, mock( SessionResourcesHandler.class ), bookmark );

        Map<String,Value> expectedParams = Collections.singletonMap( "bookmark", value( bookmark ) );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).run( "BEGIN", expectedParams, Collector.NO_OP );
        inOrder.verify( connection ).pullAll( Collector.NO_OP );
        inOrder.verify( connection ).sync();
    }
}
