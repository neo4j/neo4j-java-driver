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
package org.neo4j.driver.internal.async.connection;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.handlers.RoutingResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;

class RoutingConnectionTest
{
    @Test
    void shouldWrapHandlersWhenWritingSingleMessage()
    {
        testHandlersWrappingWithSingleMessage( false );
    }

    @Test
    void shouldWrapHandlersWhenWritingAndFlushingSingleMessage()
    {
        testHandlersWrappingWithSingleMessage( true );
    }

    @Test
    void shouldWrapHandlersWhenWritingMultipleMessages()
    {
        testHandlersWrappingWithMultipleMessages( false );
    }

    @Test
    void shouldWrapHandlersWhenWritingAndFlushingMultipleMessages()
    {
        testHandlersWrappingWithMultipleMessages( true );
    }

    private static void testHandlersWrappingWithSingleMessage( boolean flush )
    {
        Connection connection = mock( Connection.class );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );
        RoutingConnection routingConnection = new RoutingConnection( connection, defaultDatabase(), READ, errorHandler );

        if ( flush )
        {
            routingConnection.writeAndFlush( PULL_ALL, mock( ResponseHandler.class ) );
        }
        else
        {
            routingConnection.write( PULL_ALL, mock( ResponseHandler.class ) );
        }

        ArgumentCaptor<ResponseHandler> handlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        if ( flush )
        {
            verify( connection ).writeAndFlush( eq( PULL_ALL ), handlerCaptor.capture() );
        }
        else
        {
            verify( connection ).write( eq( PULL_ALL ), handlerCaptor.capture() );
        }

        assertThat( handlerCaptor.getValue(), instanceOf( RoutingResponseHandler.class ) );
    }

    private static void testHandlersWrappingWithMultipleMessages( boolean flush )
    {
        Connection connection = mock( Connection.class );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );
        RoutingConnection routingConnection = new RoutingConnection( connection, defaultDatabase(), READ, errorHandler );

        if ( flush )
        {
            routingConnection.writeAndFlush( PULL_ALL, mock( ResponseHandler.class ), DISCARD_ALL, mock( ResponseHandler.class ) );
        }
        else
        {
            routingConnection.write( PULL_ALL, mock( ResponseHandler.class ), DISCARD_ALL, mock( ResponseHandler.class ) );
        }

        ArgumentCaptor<ResponseHandler> handlerCaptor1 = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> handlerCaptor2 = ArgumentCaptor.forClass( ResponseHandler.class );

        if ( flush )
        {
            verify( connection ).writeAndFlush( eq( PULL_ALL ), handlerCaptor1.capture(), eq( DISCARD_ALL ), handlerCaptor2.capture() );
        }
        else
        {
            verify( connection ).write( eq( PULL_ALL ), handlerCaptor1.capture(), eq( DISCARD_ALL ), handlerCaptor2.capture() );
        }

        assertThat( handlerCaptor1.getValue(), instanceOf( RoutingResponseHandler.class ) );
        assertThat( handlerCaptor2.getValue(), instanceOf( RoutingResponseHandler.class ) );
    }
}
