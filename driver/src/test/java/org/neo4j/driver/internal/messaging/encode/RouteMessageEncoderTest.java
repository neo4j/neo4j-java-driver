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
package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.RouteMessage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class RouteMessageEncoderTest
{
    private final ValuePacker packer = mock( ValuePacker.class );
    private final RouteMessageEncoder encoder = new RouteMessageEncoder();


    @ParameterizedTest
    @ValueSource(strings = { "neo4j"})
    @NullSource
    void shouldEncodeRouteMessage(String databaseName) throws IOException
    {
        Map<String, Value> routingContext = getRoutingContext();

        encoder.encode( new RouteMessage( getRoutingContext(), databaseName ), packer );

        InOrder inOrder = inOrder( packer );

        inOrder.verify( packer ).packStructHeader( 2, (byte) 0x66 );
        inOrder.verify( packer ).pack( routingContext );
        inOrder.verify( packer ).pack( databaseName );
    }

    @Test
    void shouldThrowIllegalArgumentIfMessageIsNotRouteMessage()
    {
        Message message = mock( Message.class );

        assertThrows(IllegalArgumentException.class, () -> encoder.encode( message, packer ));
    }

    private Map<String,Value> getRoutingContext() {
        Map<String, Value> routingContext = new HashMap<>();
        routingContext.put( "ip", Values.value( "127.0.0.1" ) );
        return routingContext;
    }

}