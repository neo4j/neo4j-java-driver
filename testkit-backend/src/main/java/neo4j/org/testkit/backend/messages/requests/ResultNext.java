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
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.NoSuchRecordException;

@Setter
@Getter
public class ResultNext implements TestkitRequest
{
    private ResultNextBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        try
        {
            Result result = testkitState.getResultHolder( data.getResultId() ).getResult();
            return createResponse( result.next() );
        }
        catch ( NoSuchRecordException ignored )
        {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncResultHolder( data.getResultId() )
                           .thenCompose( resultCursorHolder -> resultCursorHolder.getResult().nextAsync() )
                           .thenApply( this::createResponseNullSafe );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return testkitState.getRxResultHolder( data.getResultId() )
                           .flatMap( resultHolder ->
                                     {
                                         CompletionStage<TestkitResponse> responseStage = getSubscriberStage( resultHolder )
                                                 .thenCompose( subscriber -> requestRecordsWhenPreviousAreConsumed( subscriber, resultHolder ) )
                                                 .thenCompose( subscriber -> consumeNextRecordOrCompletionSignal( subscriber, resultHolder ) )
                                                 .thenApply( this::createResponseNullSafe );
                                         return Mono.fromCompletionStage( responseStage );
                                     } );
    }

    private CompletionStage<RxBlockingSubscriber<Record>> getSubscriberStage( RxResultHolder resultHolder )
    {
        return resultHolder.getSubscriber()
                           .<CompletionStage<RxBlockingSubscriber<Record>>>map( CompletableFuture::completedFuture )
                           .orElseGet( () ->
                                       {
                                           RxBlockingSubscriber<Record> subscriber = new RxBlockingSubscriber<>();
                                           CompletionStage<RxBlockingSubscriber<Record>> subscriberStage =
                                                   subscriber.getSubscriptionStage()
                                                             .thenApply( subscription ->
                                                                         {
                                                                             resultHolder.setSubscriber( subscriber );
                                                                             return subscriber;
                                                                         } );
                                           resultHolder.getResult().records().subscribe( subscriber );
                                           return subscriberStage;
                                       } );
    }

    private CompletionStage<RxBlockingSubscriber<Record>> requestRecordsWhenPreviousAreConsumed(
            RxBlockingSubscriber<Record> subscriber, RxResultHolder resultHolder
    )
    {
        return resultHolder.getRequestedRecordsCounter().get() > 0
               ? CompletableFuture.completedFuture( subscriber )
               : subscriber.getSubscriptionStage()
                           .thenApply( subscription ->
                                       {
                                           long fetchSize = getFetchSize( resultHolder );
                                           subscription.request( fetchSize );
                                           resultHolder.getRequestedRecordsCounter().addAndGet( fetchSize );
                                           return subscriber;
                                       } );
    }

    private CompletionStage<Record> consumeNextRecord( RxBlockingSubscriber<Record> subscriber, RxResultHolder resultHolder )
    {
        CompletableFuture<Record> recordConsumer = new CompletableFuture<>();
        subscriber.setNextSignalConsumer( recordConsumer );
        return recordConsumer.thenApply( record ->
                                         {
                                             resultHolder.getRequestedRecordsCounter().decrementAndGet();
                                             return record;
                                         } );
    }

    private CompletionStage<Record> consumeNextRecordOrCompletionSignal( RxBlockingSubscriber<Record> subscriber, RxResultHolder resultHolder )
    {
        return CompletableFuture.anyOf(
                consumeNextRecord( subscriber, resultHolder ).toCompletableFuture(),
                subscriber.getCompletionStage().toCompletableFuture()
        ).thenApply( Record.class::cast );
    }

    private long getFetchSize( RxResultHolder resultHolder )
    {
        return resultHolder.getSessionHolder().getConfig()
                           .fetchSize()
                           .orElse( resultHolder.getSessionHolder().getDriverHolder().getConfig().fetchSize() );
    }

    private neo4j.org.testkit.backend.messages.responses.Record createResponse( Record record )
    {
        return neo4j.org.testkit.backend.messages.responses.Record.builder()
                                                                  .data( neo4j.org.testkit.backend.messages.responses.Record.RecordBody.builder()
                                                                                                                                       .values( record )
                                                                                                                                       .build() )
                                                                  .build();
    }

    private neo4j.org.testkit.backend.messages.responses.TestkitResponse createResponseNullSafe( Record record )
    {
        return record != null ? createResponse( record ) : NullRecord.builder().build();
    }

    @Setter
    @Getter
    public static class ResultNextBody
    {
        private String resultId;
    }
}
