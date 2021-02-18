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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.util.MetadataExtractor;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;

class RunResponseHandlerTest
{
    @Test
    void shouldNotifyCompletionFutureOnSuccess() throws Exception
    {
        CompletableFuture<Throwable> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler( runCompletedFuture );

        assertFalse( runCompletedFuture.isDone() );
        handler.onSuccess( emptyMap() );

        assertTrue( runCompletedFuture.isDone() );
        assertNull( runCompletedFuture.get() );
    }

    @Test
    void shouldNotifyCompletionFutureOnFailure() throws Exception
    {
        CompletableFuture<Throwable> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler( runCompletedFuture );

        assertFalse( runCompletedFuture.isDone() );
        handler.onFailure( new RuntimeException() );

        assertTrue( runCompletedFuture.isDone() );
        assertNotNull( runCompletedFuture.get() );
    }

    @Test
    void shouldThrowOnRecord()
    {
        RunResponseHandler handler = newHandler();

        assertThrows( UnsupportedOperationException.class, () -> handler.onRecord( values( "a", "b", "c" ) ) );
    }

    @Test
    void shouldReturnNoKeysWhenFailed()
    {
        RunResponseHandler handler = newHandler();

        handler.onFailure( new RuntimeException() );

        assertEquals( emptyList(), handler.queryKeys().keys() );
        assertEquals( emptyMap(), handler.queryKeys().keyIndex() );
    }

    @Test
    void shouldReturnDefaultResultAvailableAfterWhenFailed()
    {
        RunResponseHandler handler = newHandler();

        handler.onFailure( new RuntimeException() );

        assertEquals( -1, handler.resultAvailableAfter() );
    }

    @Test
    void shouldReturnKeysWhenSucceeded()
    {
        RunResponseHandler handler = newHandler();

        List<String> keys = asList( "key1", "key2", "key3" );
        Map<String, Integer> keyIndex = new HashMap<>();
        keyIndex.put( "key1", 0 );
        keyIndex.put( "key2", 1 );
        keyIndex.put( "key3", 2 );
        handler.onSuccess( singletonMap( "fields", value( keys ) ) );

        assertEquals( keys, handler.queryKeys().keys() );
        assertEquals( keyIndex, handler.queryKeys().keyIndex() );
    }


    @Test
    void shouldReturnResultAvailableAfterWhenSucceededV3()
    {
        testResultAvailableAfterOnSuccess( "t_first", BoltProtocolV3.METADATA_EXTRACTOR );
    }

    private static void testResultAvailableAfterOnSuccess( String key, MetadataExtractor metadataExtractor )
    {
        RunResponseHandler handler = newHandler( metadataExtractor );

        handler.onSuccess( singletonMap( key, value( 42 ) ) );

        assertEquals( 42L, handler.resultAvailableAfter() );
    }

    private static RunResponseHandler newHandler()
    {
        return newHandler( BoltProtocolV3.METADATA_EXTRACTOR );
    }

    private static RunResponseHandler newHandler( CompletableFuture<Throwable> runCompletedFuture )
    {
        return newHandler( runCompletedFuture, BoltProtocolV3.METADATA_EXTRACTOR );
    }

    private static RunResponseHandler newHandler( MetadataExtractor metadataExtractor )
    {
        return newHandler( new CompletableFuture<>(), metadataExtractor );
    }

    private static RunResponseHandler newHandler( CompletableFuture<Throwable> runCompletedFuture, MetadataExtractor metadataExtractor )
    {
        return new RunResponseHandler( runCompletedFuture, metadataExtractor );
    }
}
