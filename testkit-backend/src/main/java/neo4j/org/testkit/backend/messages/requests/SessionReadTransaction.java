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
package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.AsyncSessionState;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RetryableDone;
import neo4j.org.testkit.backend.messages.responses.RetryableTry;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.exceptions.Neo4jException;

@Setter
@Getter
public class SessionReadTransaction implements TestkitRequest
{
    private SessionReadTransactionBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return Optional.ofNullable( testkitState.getSessionStates().getOrDefault( data.getSessionId(), null ) )
                       .map( sessionState ->
                             {
                                 Session session = sessionState.getSession();
                                 session.readTransaction( handle( testkitState, sessionState ) );
                                 return retryableDone();
                             } ).orElseThrow( () -> new RuntimeException( "Could not find session" ) );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        AsyncSessionState sessionState = testkitState.getAsyncSessionStates().get( data.getSessionId() );
        AsyncSession session = sessionState.getSession();

        AsyncTransactionWork<CompletionStage<Void>> workWrapper = tx ->
        {
            String txId = testkitState.addAsyncTransaction( tx );
            testkitState.getResponseWriter().accept( retryableTry( txId ) );
            CompletableFuture<Void> txWorkFuture = new CompletableFuture<>();
            sessionState.setTxWorkFuture( txWorkFuture );
            return txWorkFuture;
        };

        return session.readTransactionAsync( workWrapper )
                      .thenApply( nothing -> retryableDone() );
    }

    private TransactionWork<Void> handle( TestkitState testkitState, SessionState sessionState )
    {
        return tx ->
        {
            String txId = testkitState.addTransaction( tx );
            testkitState.getResponseWriter().accept( retryableTry( txId ) );
            CompletableFuture<Void> txWorkFuture = new CompletableFuture<>();
            sessionState.setTxWorkFuture( txWorkFuture );

            try
            {
                return txWorkFuture.get();
            }
            catch ( Throwable throwable )
            {
                Throwable workThrowable = throwable;
                if ( workThrowable instanceof ExecutionException )
                {
                    workThrowable = workThrowable.getCause();
                }
                if ( workThrowable instanceof Neo4jException )
                {
                    throw (Neo4jException) workThrowable;
                }
                else
                {
                    throw new RuntimeException( "Unexpected exception occurred in transaction work function", workThrowable );
                }
            }
        };
    }

    private RetryableTry retryableTry( String txId )
    {
        return RetryableTry.builder().data( RetryableTry.RetryableTryBody.builder().id( txId ).build() ).build();
    }

    private RetryableDone retryableDone()
    {
        return RetryableDone.builder().build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class SessionReadTransactionBody
    {
        private String sessionId;
    }
}
