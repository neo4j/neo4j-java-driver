/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static java.util.Collections.EMPTY_MAP;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class StandardTransactionTest
{
    @Test
    public void shouldRollbackOnNoExplicitSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "ROLLBACK", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldRollbackOnExplicitFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        tx.failure();
        tx.success(); // even if success is called after the failure call!

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "ROLLBACK", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldCommitOnSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        tx.success();

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "COMMIT", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).sync();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }
}
