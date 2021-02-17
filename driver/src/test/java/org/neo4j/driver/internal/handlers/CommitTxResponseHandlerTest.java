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

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalBookmark;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.util.TestUtil.await;

class CommitTxResponseHandlerTest
{
    private final CompletableFuture<Bookmark> future = new CompletableFuture<>();
    private final CommitTxResponseHandler handler = new CommitTxResponseHandler( future );

    @Test
    void shouldHandleSuccessWithoutBookmark()
    {
        handler.onSuccess( emptyMap() );

        assertNull( await( future ) );
    }

    @Test
    void shouldHandleSuccessWithBookmark()
    {
        String bookmarkString = "neo4j:bookmark:v1:tx12345";

        handler.onSuccess( singletonMap( "bookmark", value( bookmarkString ) ) );

        assertEquals( InternalBookmark.parse( bookmarkString ), await( future ) );
    }

    @Test
    void shouldHandleFailure()
    {
        RuntimeException error = new RuntimeException( "Hello" );

        handler.onFailure( error );

        RuntimeException receivedError = assertThrows( RuntimeException.class, () -> await( future ) );
        assertEquals( error, receivedError );
    }

    @Test
    void shouldFailToHandleRecord()
    {
        assertThrows( UnsupportedOperationException.class, () -> handler.onRecord( new Value[]{value( 42 )} ) );
    }
}
