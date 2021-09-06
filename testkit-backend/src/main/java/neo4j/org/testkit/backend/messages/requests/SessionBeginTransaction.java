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
import neo4j.org.testkit.backend.AsyncSessionState;
import neo4j.org.testkit.backend.RxSessionState;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.RxSession;

@Setter
@Getter
public class SessionBeginTransaction implements TestkitRequest
{
    private SessionBeginTransactionBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return Optional.ofNullable( testkitState.getSessionStates().getOrDefault( data.sessionId, null ) )
                       .map( SessionState::getSession )
                       .map( session ->
                             {
                                 TransactionConfig.Builder builder = TransactionConfig.builder();
                                 Optional.ofNullable( data.txMeta ).ifPresent( builder::withMetadata );

                                 if ( data.getTimeout() != null )
                                 {
                                     builder.withTimeout( Duration.ofMillis( data.getTimeout() ) );
                                 }

                                 return transaction( testkitState.addTransaction( session.beginTransaction( builder.build() ) ) );
                             } )
                       .orElseThrow( () -> new RuntimeException( "Could not find session" ) );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        AsyncSessionState sessionState = testkitState.getAsyncSessionStates().get( data.getSessionId() );
        if ( sessionState != null )
        {
            AsyncSession session = sessionState.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();
            Optional.ofNullable( data.txMeta ).ifPresent( builder::withMetadata );

            if ( data.getTimeout() != null )
            {
                builder.withTimeout( Duration.ofMillis( data.getTimeout() ) );
            }

            return session.beginTransactionAsync( builder.build() ).thenApply( tx -> transaction( testkitState.addAsyncTransaction( tx ) ) );
        }
        else
        {
            CompletableFuture<TestkitResponse> future = new CompletableFuture<>();
            future.completeExceptionally( new RuntimeException( "Could not find session" ) );
            return future;
        }
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        RxSessionState sessionState = testkitState.getRxSessionStates().get( data.getSessionId() );
        if ( sessionState != null )
        {
            RxSession session = sessionState.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();
            Optional.ofNullable( data.txMeta ).ifPresent( builder::withMetadata );

            if ( data.getTimeout() != null )
            {
                builder.withTimeout( Duration.ofMillis( data.getTimeout() ) );
            }

            return Mono.fromDirect( session.beginTransaction( builder.build() ) )
                       .map( tx -> transaction( testkitState.addRxTransaction( tx ) ) );
        }
        else
        {
            return Mono.error( new RuntimeException( "Could not find session" ) );
        }
    }

    private Transaction transaction( String txId )
    {
        return Transaction.builder().data( Transaction.TransactionBody.builder().id( txId ).build() ).build();
    }

    @Getter
    @Setter
    public static class SessionBeginTransactionBody
    {
        private String sessionId;
        private Map<String,Object> txMeta;
        private Integer timeout;
    }
}
