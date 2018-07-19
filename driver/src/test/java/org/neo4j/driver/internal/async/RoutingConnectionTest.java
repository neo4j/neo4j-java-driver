/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.handlers.RoutingResponseHandler;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.AccessMode.READ;

class RoutingConnectionTest
{
    @Test
    void shouldWrapGivenHandlersInRun()
    {
        testHandlersWrapping( false );
    }

    @Test
    void shouldWrapGivenHandlersInRunAndFlush()
    {
        testHandlersWrapping( true );
    }

    private static void testHandlersWrapping( boolean flush )
    {
        Connection connection = mock( Connection.class );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );
        RoutingConnection routingConnection = new RoutingConnection( connection, READ, errorHandler );

        if ( flush )
        {
            routingConnection.writeAndFlush( new RunMessage( "RETURN 1" ), mock( ResponseHandler.class ),
                    PullAllMessage.PULL_ALL, mock( ResponseHandler.class ) );
        }
        else
        {
            routingConnection.write( new RunMessage( "RETURN 1" ), mock( ResponseHandler.class ),
                    PullAllMessage.PULL_ALL, mock( ResponseHandler.class ) );
        }

        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullAllHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        if ( flush )
        {
            verify( connection ).writeAndFlush( eq( new RunMessage( "RETURN 1" ) ), runHandlerCaptor.capture(),
                    eq( PullAllMessage.PULL_ALL ), pullAllHandlerCaptor.capture() );
        }
        else
        {
            verify( connection ).write( eq( new RunMessage( "RETURN 1" ) ), runHandlerCaptor.capture(),
                    eq( PullAllMessage.PULL_ALL ), pullAllHandlerCaptor.capture() );
        }

        assertThat( runHandlerCaptor.getValue(), instanceOf( RoutingResponseHandler.class ) );
        assertThat( pullAllHandlerCaptor.getValue(), instanceOf( RoutingResponseHandler.class ) );
    }
}
