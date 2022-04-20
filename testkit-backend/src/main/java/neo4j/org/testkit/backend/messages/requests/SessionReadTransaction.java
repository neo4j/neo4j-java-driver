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
import lombok.Setter;
import neo4j.org.testkit.backend.FrontendError;
import neo4j.org.testkit.backend.ReactiveTransactionContextAdapter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionHolder;
import neo4j.org.testkit.backend.holder.RxTransactionHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.responses.RetryableDone;
import neo4j.org.testkit.backend.messages.responses.RetryableTry;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.reactive.ReactiveTransactionCallback;
import org.neo4j.driver.reactive.RxTransactionWork;

@Setter
@Getter
public class SessionReadTransaction implements TestkitRequest
{
    private SessionReadTransactionBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        SessionHolder sessionHolder = testkitState.getSessionHolder( data.getSessionId() );
        Session session = sessionHolder.getSession();
        session.readTransaction( handle( testkitState, sessionHolder ) );
        return retryableDone();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncSessionHolder( data.getSessionId() )
                           .thenCompose( sessionHolder ->
                                         {
                                             AsyncSession session = sessionHolder.getSession();

                                             AsyncTransactionWork<CompletionStage<Void>> workWrapper = tx ->
                                             {
                                                 String txId = testkitState.addAsyncTransactionHolder( new AsyncTransactionHolder( sessionHolder, tx ) );
                                                 testkitState.getResponseWriter().accept( retryableTry( txId ) );
                                                 CompletableFuture<Void> txWorkFuture = new CompletableFuture<>();
                                                 sessionHolder.setTxWorkFuture( txWorkFuture );
                                                 return txWorkFuture;
                                             };

                                             return session.readTransactionAsync( workWrapper );
                                         } )
                           .thenApply( nothing -> retryableDone() );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return testkitState.getRxSessionHolder( data.getSessionId() )
                           .flatMap( sessionHolder ->
                                     {
                                         RxTransactionWork<Publisher<Void>> workWrapper = tx ->
                                         {
                                             String txId = testkitState.addRxTransactionHolder( new RxTransactionHolder( sessionHolder, tx ) );
                                             testkitState.getResponseWriter().accept( retryableTry( txId ) );
                                             CompletableFuture<Void> tryResult = new CompletableFuture<>();
                                             sessionHolder.setTxWorkFuture( tryResult );
                                             return Mono.fromCompletionStage( tryResult );
                                         };

                                         return Mono.fromDirect( sessionHolder.getSession().readTransaction( workWrapper ) );
                                     } )
                           .then( Mono.just( retryableDone() ) );
    }

    @Override
    public Mono<TestkitResponse> processReactive( TestkitState testkitState )
    {
        return testkitState.getReactiveSessionHolder( data.getSessionId() )
                           .flatMap( sessionHolder ->
                                     {
                                         ReactiveTransactionCallback<Publisher<Void>> workWrapper = tx ->
                                         {
                                             String txId = testkitState.addReactiveTransactionHolder(
                                                     new ReactiveTransactionHolder( sessionHolder, new ReactiveTransactionContextAdapter( tx ) ) );
                                             testkitState.getResponseWriter().accept( retryableTry( txId ) );
                                             CompletableFuture<Void> tryResult = new CompletableFuture<>();
                                             sessionHolder.setTxWorkFuture( tryResult );
                                             return Mono.fromCompletionStage( tryResult );
                                         };

                                         return Mono.fromDirect( sessionHolder.getSession().executeRead( workWrapper ) );
                                     } )
                           .then( Mono.just( retryableDone() ) );
    }

    private TransactionWork<Void> handle( TestkitState testkitState, SessionHolder sessionHolder )
    {
        return tx ->
        {
            String txId = testkitState.addTransactionHolder( new TransactionHolder( sessionHolder, tx ) );
            testkitState.getResponseWriter().accept( retryableTry( txId ) );
            CompletableFuture<Void> txWorkFuture = new CompletableFuture<>();
            sessionHolder.setTxWorkFuture( txWorkFuture );

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
                if ( workThrowable instanceof FrontendError )
                {
                    throw (FrontendError) workThrowable;
                }
                throw new RuntimeException( "Unexpected exception occurred in transaction work function", workThrowable );
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
    public static class SessionReadTransactionBody
    {
        private String sessionId;
    }
}
