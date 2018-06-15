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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AckFailureResponseHandlerTest
{
    private final InboundMessageDispatcher dispatcher = mock( InboundMessageDispatcher.class );
    private final AckFailureResponseHandler handler = new AckFailureResponseHandler( dispatcher );

    @Test
    void shouldClearCurrentErrorOnSuccess()
    {
        verify( dispatcher, never() ).clearCurrentError();
        handler.onSuccess( emptyMap() );
        verify( dispatcher ).clearCurrentError();
    }

    @Test
    void shouldThrowOnFailure()
    {
        RuntimeException error = new RuntimeException( "Unable to process ACK_FAILURE" );

        ClientException e = assertThrows( ClientException.class, () -> handler.onFailure( error ) );
        assertSame( error, e.getCause() );
    }

    @Test
    void shouldClearCurrentErrorWhenAckFailureMutedAndFailureReceived()
    {
        RuntimeException error = new RuntimeException( "Some error" );
        when( dispatcher.isAckFailureMuted() ).thenReturn( true );

        handler.onFailure( error );

        verify( dispatcher ).clearCurrentError();
    }

    @Test
    void shouldThrowOnRecord()
    {
        assertThrows( UnsupportedOperationException.class, () -> handler.onRecord( new Value[0] ) );
    }
}
