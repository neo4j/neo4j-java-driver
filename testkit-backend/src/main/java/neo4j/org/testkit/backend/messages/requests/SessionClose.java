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
import neo4j.org.testkit.backend.RxBlockingSubscriber;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.RxResultHolder;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.Record;

@Setter
@Getter
public class SessionClose implements TestkitRequest
{
    private SessionCloseBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        testkitState.getSessionHolder( data.getSessionId() ).getSession().close();
        return createResponse();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncSessionHolder( data.getSessionId() )
                           .thenCompose( sessionHolder -> sessionHolder.getSession().closeAsync() )
                           .thenApply( ignored -> createResponse() );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return testkitState.getRxSessionHolder( data.getSessionId() )
                           .flatMap( sessionHolder -> sessionHolder.getResultHolder()
                                                                   .map( this::consumeRequestedDemandAndCancelIfSubscribed )
                                                                   .orElse( Mono.empty() )
                                                                   .then( Mono.fromDirect( sessionHolder.getSession().close() ) ) )
                           .then( Mono.just( createResponse() ) );
    }

    private Mono<Void> consumeRequestedDemandAndCancelIfSubscribed( RxResultHolder resultHolder )
    {
        return resultHolder.getSubscriber()
                           .map( subscriber -> Mono.fromCompletionStage( consumeRequestedDemandAndCancelIfSubscribed( resultHolder, subscriber ) ) )
                           .orElse( Mono.empty() );
    }

    private CompletionStage<Void> consumeRequestedDemandAndCancelIfSubscribed( RxResultHolder resultHolder, RxBlockingSubscriber<Record> subscriber )
    {
        if ( subscriber.getCompletionStage().toCompletableFuture().isDone() )
        {
            return CompletableFuture.completedFuture( null );
        }

        return new DemandConsumer<>( subscriber, resultHolder.getRequestedRecordsCounter() )
                .getCompletedStage()
                .thenCompose( completionReason ->
                              {
                                  CompletionStage<Void> result;
                                  switch ( completionReason )
                                  {
                                  case REQUESTED_DEMAND_CONSUMED:
                                      result = subscriber.getSubscriptionStage().thenApply( subscription ->
                                                                                            {
                                                                                                subscription.cancel();
                                                                                                return null;
                                                                                            } );
                                      break;
                                  case RECORD_STREAM_EXHAUSTED:
                                      result = CompletableFuture.completedFuture( null );
                                      break;
                                  default:
                                      result = new CompletableFuture<>();
                                      result.toCompletableFuture()
                                            .completeExceptionally( new RuntimeException( "Unexpected completion reason: " + completionReason ) );
                                  }
                                  return result;
                              } );
    }

    private Session createResponse()
    {
        return Session.builder().data( Session.SessionBody.builder().id( data.getSessionId() ).build() ).build();
    }

    private static class DemandConsumer<T>
    {
        private final RxBlockingSubscriber<T> subscriber;
        private final AtomicLong unfulfilledDemandCounter;
        @Getter
        private final CompletableFuture<CompletionReason> completedStage = new CompletableFuture<>();

        private enum CompletionReason
        {
            REQUESTED_DEMAND_CONSUMED,
            RECORD_STREAM_EXHAUSTED
        }

        private DemandConsumer( RxBlockingSubscriber<T> subscriber, AtomicLong unfulfilledDemandCounter )
        {
            this.subscriber = subscriber;
            this.unfulfilledDemandCounter = unfulfilledDemandCounter;

            subscriber.getCompletionStage().whenComplete( this::onComplete );
            if ( this.unfulfilledDemandCounter.get() > 0 )
            {
                setupNextSignalConsumer();
            }
        }

        private void setupNextSignalConsumer()
        {
            CompletableFuture<T> consumer = new CompletableFuture<>();
            subscriber.setNextSignalConsumer( consumer );
            consumer.whenComplete( this::onNext );
        }

        private void onNext( T ignored, Throwable throwable )
        {
            if ( throwable != null )
            {
                completedStage.completeExceptionally( throwable );
                return;
            }

            if ( unfulfilledDemandCounter.decrementAndGet() > 0 )
            {
                setupNextSignalConsumer();
            }
            else
            {
                completedStage.complete( CompletionReason.REQUESTED_DEMAND_CONSUMED );
            }
        }

        private void onComplete( Void ignored, Throwable throwable )
        {
            if ( throwable != null )
            {
                completedStage.completeExceptionally( throwable );
            }
            else
            {
                completedStage.complete( CompletionReason.RECORD_STREAM_EXHAUSTED );
            }
        }
    }

    @Setter
    @Getter
    private static class SessionCloseBody
    {
        private String sessionId;
    }
}
