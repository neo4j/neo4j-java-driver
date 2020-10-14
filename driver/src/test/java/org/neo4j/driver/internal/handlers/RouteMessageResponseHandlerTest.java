/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RouteMessageResponseHandlerTest
{

    @Test
    void onSuccessShouldSuccessFullyCompleteFutureWithRoutingTable() throws Exception
    {
        CompletableFuture<Map<String,Value>> completableFuture = new CompletableFuture<>();
        RouteMessageResponseHandler responseHandler = new RouteMessageResponseHandler( completableFuture );
        Map<String,Value> routingTable = getRoutingTable();
        Map<String,Value> metadata = getMetadataWithRoutingTable( routingTable );

        responseHandler.onSuccess( metadata );

        assertEquals( routingTable, completableFuture.getNow( null ) );
    }

    @Test
    void onSuccessShouldExceptionallyCompleteFutureWhenMetadataDoesNotHaveRoutingTable() throws Exception
    {
        CompletableFuture<Map<String,Value>> completableFuture = new CompletableFuture<>();
        RouteMessageResponseHandler responseHandler = new RouteMessageResponseHandler( completableFuture );
        Map<String,Value> metadata = new HashMap<>();

        responseHandler.onSuccess( metadata );

        assertThrows( CompletionException.class, () -> completableFuture.getNow( null ) );
    }

    @Test
    void onFailureShouldCompleteExceptionallyWithTheOriginalException()
    {
        CompletableFuture<Map<String,Value>> completableFuture = new CompletableFuture<>();
        RouteMessageResponseHandler responseHandler = new RouteMessageResponseHandler( completableFuture );
        RuntimeException expectedException = new RuntimeException( "Test exception" );

        responseHandler.onFailure( expectedException );

        assertTrue( completableFuture.isCompletedExceptionally() );
        completableFuture.handle( ( value, ex ) ->
                                  {
                                      assertNull( value );
                                      assertEquals( expectedException, ex );
                                      return null;
                                  } );
    }

    @Test
    void onRecordShouldThrowUnsupportedOperation()
    {
        CompletableFuture<Map<String,Value>> completableFuture = new CompletableFuture<>();
        RouteMessageResponseHandler responseHandler = new RouteMessageResponseHandler( completableFuture );

        responseHandler.onRecord( new Value[0] );

        assertTrue( completableFuture.isCompletedExceptionally() );
        completableFuture.handle( ( value, ex ) ->
                                  {
                                      assertNull( value );
                                      assertEquals( UnsupportedOperationException.class, ex.getClass() );
                                      return null;
                                  } );
    }

    private Map<String,Value> getMetadataWithRoutingTable( Map<String,Value> routingTable )
    {
        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "rt", Values.value( routingTable ) );
        return metadata;
    }

    private Map<String,Value> getRoutingTable()
    {
        Map<String,Value> routingTable = new HashMap<>();
        routingTable.put( "ttl", Values.value( 300 ) );
        routingTable.put( "addresses", Values.value( new ArrayList<>() ) );
        return routingTable;
    }
}