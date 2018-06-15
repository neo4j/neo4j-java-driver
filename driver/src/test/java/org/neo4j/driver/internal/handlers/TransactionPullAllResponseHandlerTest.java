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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransactionPullAllResponseHandlerTest
{
    @Test
    void shouldMarkTransactionAsFailedOnNonFatalFailures()
    {
        testErrorHandling( new ClientException( "Neo.ClientError.Cluster.NotALeader", "" ), false );
        testErrorHandling( new ClientException( "Neo.ClientError.Procedure.ProcedureCallFailed", "" ), false );
        testErrorHandling( new TransientException( "Neo.TransientError.Transaction.Terminated", "" ), false );
        testErrorHandling( new TransientException( "Neo.TransientError.General.DatabaseUnavailable", "" ), false );
    }

    @Test
    void shouldMarkTransactionAsTerminatedOnFatalFailures()
    {
        testErrorHandling( new RuntimeException(), true );
        testErrorHandling( new IOException(), true );
        testErrorHandling( new ServiceUnavailableException( "" ), true );
        testErrorHandling( new SessionExpiredException( "" ), true );
        testErrorHandling( new SessionExpiredException( "" ), true );
        testErrorHandling( new ClientException( "Neo.ClientError.Request.Invalid" ), true );
    }

    private static void testErrorHandling( Throwable error, boolean fatal )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        ExplicitTransaction tx = mock( ExplicitTransaction.class );
        TransactionPullAllResponseHandler handler = new TransactionPullAllResponseHandler( new Statement( "RETURN 1" ),
                new RunResponseHandler( new CompletableFuture<>() ), connection, tx );

        handler.onFailure( error );

        if ( fatal )
        {
            verify( tx ).markTerminated();
        }
        else
        {
            verify( tx ).failure();
        }
    }
}
