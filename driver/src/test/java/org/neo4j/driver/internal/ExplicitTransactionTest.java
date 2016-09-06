/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Collections;

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Value;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
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
        Runnable cleanup = mock( Runnable.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, cleanup );

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
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldRollbackOnExplicitFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        when( conn.isOpen() ).thenReturn( true );
        Runnable cleanup = mock( Runnable.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, cleanup );

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
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldCommitOnSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        when( conn.isOpen() ).thenReturn( true );
        Runnable cleanup = mock( Runnable.class );
        ExplicitTransaction tx = new ExplicitTransaction( conn, cleanup );

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
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }
}
