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
package neo4j.org.testkit.backend;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class RxBlockingSubscriber<T> implements Subscriber<T>
{
    private final CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();
    private CompletableFuture<CompletableFuture<T>> nextSignalConsumerFuture;

    public void setNextSignalConsumer( CompletableFuture<T> nextSignalConsumer )
    {
        nextSignalConsumerFuture.complete( nextSignalConsumer );
    }

    public CompletionStage<Subscription> getSubscriptionStage()
    {
        return subscriptionFuture;
    }

    public CompletionStage<Void> getCompletionStage()
    {
        return completionFuture;
    }

    @Override
    public void onSubscribe( Subscription s )
    {
        nextSignalConsumerFuture = new CompletableFuture<>();
        subscriptionFuture.complete( s );
    }

    @Override
    public void onNext( T t )
    {
        blockUntilNextSignalConsumer().complete( t );
    }

    @Override
    public void onError( Throwable t )
    {
        completionFuture.completeExceptionally( t );
    }

    @Override
    public void onComplete()
    {
        completionFuture.complete( null );
    }

    private CompletableFuture<T> blockUntilNextSignalConsumer()
    {
        CompletableFuture<T> nextSignalConsumer;
        try
        {
            nextSignalConsumer = nextSignalConsumerFuture.get();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( "Failed waiting for next signal consumer", throwable );
        }
        nextSignalConsumerFuture = new CompletableFuture<>();
        return nextSignalConsumer;
    }
}
