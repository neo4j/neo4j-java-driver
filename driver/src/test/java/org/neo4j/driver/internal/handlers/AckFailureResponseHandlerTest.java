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

import org.junit.Test;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AckFailureResponseHandlerTest
{
    private final InboundMessageDispatcher dispatcher = mock( InboundMessageDispatcher.class );
    private final AckFailureResponseHandler handler = new AckFailureResponseHandler( dispatcher );

    @Test
    public void shouldClearCurrentErrorOnSuccess()
    {
        verify( dispatcher, never() ).clearCurrentError();
        handler.onSuccess( emptyMap() );
        verify( dispatcher ).clearCurrentError();
    }

    @Test
    public void shouldThrowOnFailure()
    {
        RuntimeException error = new RuntimeException( "Unable to process ACK_FAILURE" );

        try
        {
            handler.onFailure( error );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertSame( error, e.getCause() );
        }
    }

    @Test
    public void shouldClearCurrentErrorWhenAckFailureMutedAndFailureReceived()
    {
        RuntimeException error = new RuntimeException( "Some error" );
        when( dispatcher.isAckFailureMuted() ).thenReturn( true );

        handler.onFailure( error );

        verify( dispatcher ).clearCurrentError();
    }

    @Test
    public void shouldThrowOnRecord()
    {
        try
        {
            handler.onRecord( new Value[0] );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }
}
