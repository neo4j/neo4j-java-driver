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
package org.neo4j.driver.internal.retry;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.util.Experimental;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ExponentialBackoffRetryLogic implements RetryLogic
{
    private final static String RETRY_LOGIC_LOG_NAME = "RetryLogic";

    static final long DEFAULT_MAX_RETRY_TIME_MS = SECONDS.toMillis( 30 );

    private static final long INITIAL_RETRY_DELAY_MS = SECONDS.toMillis( 1 );
    private static final double RETRY_DELAY_MULTIPLIER = 2.0;
    private static final double RETRY_DELAY_JITTER_FACTOR = 0.2;
    private static final long MAX_RETRY_DELAY = Long.MAX_VALUE / 2;

    private final long maxRetryTimeMs;
    private final long initialRetryDelayMs;
    private final double multiplier;
    private final double jitterFactor;
    private final EventExecutorGroup eventExecutorGroup;
    private final Clock clock;
    private final Logger log;

    public ExponentialBackoffRetryLogic( RetrySettings settings, EventExecutorGroup eventExecutorGroup, Clock clock,
            Logging logging )
    {
        this( settings.maxRetryTimeMs(), INITIAL_RETRY_DELAY_MS, RETRY_DELAY_MULTIPLIER, RETRY_DELAY_JITTER_FACTOR,
                eventExecutorGroup, clock, logging );
    }

    ExponentialBackoffRetryLogic( long maxRetryTimeMs, long initialRetryDelayMs, double multiplier,
            double jitterFactor, EventExecutorGroup eventExecutorGroup, Clock clock, Logging logging )
    {
        this.maxRetryTimeMs = maxRetryTimeMs;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
        this.eventExecutorGroup = eventExecutorGroup;
        this.clock = clock;
        this.log = logging.getLog( RETRY_LOGIC_LOG_NAME );

        verifyAfterConstruction();
    }

    @Override
    public <T> T retry( Supplier<T> work )
    {
        List<Throwable> errors = null;
        long startTime = -1;
        long nextDelayMs = initialRetryDelayMs;

        while ( true )
        {
            try
            {
                return work.get();
            }
            catch ( Throwable throwable )
            {
                Throwable error = extractPossibleTerminationCause( throwable );
                if ( canRetryOn( error ) )
                {
                    long currentTime = clock.millis();
                    if ( startTime == -1 )
                    {
                        startTime = currentTime;
                    }

                    long elapsedTime = currentTime - startTime;
                    if ( elapsedTime < maxRetryTimeMs )
                    {
                        long delayWithJitterMs = computeDelayWithJitter( nextDelayMs );
                        log.warn( "Transaction failed and will be retried in " + delayWithJitterMs + "ms", error );

                        sleep( delayWithJitterMs );
                        nextDelayMs = (long) (nextDelayMs * multiplier);
                        errors = recordError( error, errors );
                        continue;
                    }
                }

                // Add the original error in case we didn't continue the loop from within the if above.
                addSuppressed( throwable, errors );
                throw throwable;
            }
        }
    }

    @Override
    public <T> CompletionStage<T> retryAsync( Supplier<CompletionStage<T>> work )
    {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        executeWorkInEventLoop( resultFuture, work );
        return resultFuture;
    }

    @Override
    public <T> Publisher<T> retryRx( Publisher<T> work )
    {
        return Flux.from( work ).retryWhen( retryRxCondition() );
    }

    protected boolean canRetryOn( Throwable error )
    {
        return isRetryable( error );
    }

    @Experimental
    public static boolean isRetryable( Throwable error )
    {
        return error instanceof SessionExpiredException || error instanceof ServiceUnavailableException || isTransientError( error );
    }

    /**
     * Extracts the possible cause of a transaction that has been marked terminated.
     *
     * @param error
     * @return
     */
    private static Throwable extractPossibleTerminationCause( Throwable error )
    {

        // Having a dedicated "TerminatedException" inheriting from ClientException might be a good idea.
        if ( error instanceof ClientException && error.getCause() != null )
        {
            return error.getCause();
        }
        return error;
    }

    private Function<Flux<Throwable>,Publisher<Context>> retryRxCondition()
    {
        return errorCurrentAttempt -> errorCurrentAttempt.flatMap( e -> Mono.subscriberContext().map( ctx -> Tuples.of( e, ctx ) ) ).flatMap( t2 ->
        {

            Throwable throwable = t2.getT1();
            Throwable error = extractPossibleTerminationCause( throwable );

            Context ctx = t2.getT2();

            List<Throwable> errors = ctx.getOrDefault( "errors", null );

            long startTime = ctx.getOrDefault( "startTime", -1L );
            long nextDelayMs = ctx.getOrDefault( "nextDelayMs", initialRetryDelayMs );

            if ( canRetryOn( error ) )
            {
                long currentTime = clock.millis();
                if ( startTime == -1 )
                {
                    startTime = currentTime;
                }

                long elapsedTime = currentTime - startTime;
                if ( elapsedTime < maxRetryTimeMs )
                {
                    long delayWithJitterMs = computeDelayWithJitter( nextDelayMs );
                    log.warn( "Reactive transaction failed and is scheduled to retry in " + delayWithJitterMs + "ms", error );

                    nextDelayMs = (long) (nextDelayMs * multiplier);
                    errors = recordError( error, errors );

                    // retry on netty event loop thread
                    EventExecutor eventExecutor = eventExecutorGroup.next();
                    return Mono.just( ctx.put( "errors", errors ).put( "startTime", startTime ).put( "nextDelayMs", nextDelayMs ) ).delayElement(
                            Duration.ofMillis( delayWithJitterMs ), Schedulers.fromExecutorService( eventExecutor ) );
                }
            }
            addSuppressed( throwable, errors );

            return Mono.error( throwable );
        } );
    }

    private <T> void executeWorkInEventLoop( CompletableFuture<T> resultFuture, Supplier<CompletionStage<T>> work )
    {
        // this is the very first time we execute given work
        EventExecutor eventExecutor = eventExecutorGroup.next();

        eventExecutor.execute( () -> executeWork( resultFuture, work, -1, initialRetryDelayMs, null ) );
    }

    private <T> void retryWorkInEventLoop( CompletableFuture<T> resultFuture, Supplier<CompletionStage<T>> work,
            Throwable error, long startTime, long delayMs, List<Throwable> errors )
    {
        // work has failed before, we need to schedule retry with the given delay
        EventExecutor eventExecutor = eventExecutorGroup.next();

        long delayWithJitterMs = computeDelayWithJitter( delayMs );
        log.warn( "Async transaction failed and is scheduled to retry in " + delayWithJitterMs + "ms", error );

        eventExecutor.schedule( () ->
        {
            long newRetryDelayMs = (long) (delayMs * multiplier);
            executeWork( resultFuture, work, startTime, newRetryDelayMs, errors );
        }, delayWithJitterMs, TimeUnit.MILLISECONDS );
    }

    private <T> void executeWork( CompletableFuture<T> resultFuture, Supplier<CompletionStage<T>> work,
            long startTime, long retryDelayMs, List<Throwable> errors )
    {
        CompletionStage<T> workStage;
        try
        {
            workStage = work.get();
        }
        catch ( Throwable error )
        {
            // work failed in a sync way, attempt to schedule a retry
            retryOnError( resultFuture, work, startTime, retryDelayMs, error, errors );
            return;
        }

        workStage.whenComplete( ( result, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                // work failed in async way, attempt to schedule a retry
                retryOnError( resultFuture, work, startTime, retryDelayMs, error, errors );
            }
            else
            {
                resultFuture.complete( result );
            }
        } );
    }

    private <T> void retryOnError( CompletableFuture<T> resultFuture, Supplier<CompletionStage<T>> work, long startTime, long retryDelayMs, Throwable throwable,
            List<Throwable> errors )
    {
        Throwable error = extractPossibleTerminationCause( throwable );
        if ( canRetryOn( error ) )
        {
            long currentTime = clock.millis();
            if ( startTime == -1 )
            {
                startTime = currentTime;
            }

            long elapsedTime = currentTime - startTime;
            if ( elapsedTime < maxRetryTimeMs )
            {
                errors = recordError( error, errors );
                retryWorkInEventLoop( resultFuture, work, error, startTime, retryDelayMs, errors );
                return;
            }
        }

        addSuppressed( throwable, errors );
        resultFuture.completeExceptionally( throwable );
    }

    private long computeDelayWithJitter( long delayMs )
    {
        if ( delayMs > MAX_RETRY_DELAY )
        {
            delayMs = MAX_RETRY_DELAY;
        }

        long jitter = (long) (delayMs * jitterFactor);
        long min = delayMs - jitter;
        long max = delayMs + jitter;
        return ThreadLocalRandom.current().nextLong( min, max + 1 );
    }

    private void sleep( long delayMs )
    {
        try
        {
            clock.sleep( delayMs );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException( "Retries interrupted", e );
        }
    }

    private void verifyAfterConstruction()
    {
        if ( maxRetryTimeMs < 0 )
        {
            throw new IllegalArgumentException( "Max retry time should be >= 0: " + maxRetryTimeMs );
        }
        if ( initialRetryDelayMs < 0 )
        {
            throw new IllegalArgumentException( "Initial retry delay should >= 0: " + initialRetryDelayMs );
        }
        if ( multiplier < 1.0 )
        {
            throw new IllegalArgumentException( "Multiplier should be >= 1.0: " + multiplier );
        }
        if ( jitterFactor < 0 || jitterFactor > 1 )
        {
            throw new IllegalArgumentException( "Jitter factor should be in [0.0, 1.0]: " + jitterFactor );
        }
        if ( clock == null )
        {
            throw new IllegalArgumentException( "Clock should not be null" );
        }
    }

    private static boolean isTransientError( Throwable error )
    {
        if ( error instanceof TransientException )
        {
            String code = ((TransientException) error).code();
            // Retries should not happen when transaction was explicitly terminated by the user.
            // Termination of transaction might result in two different error codes depending on where it was
            // terminated. These are really client errors but classification on the server is not entirely correct and
            // they are classified as transient.
            if ( "Neo.TransientError.Transaction.Terminated".equals( code ) ||
                 "Neo.TransientError.Transaction.LockClientStopped".equals( code ) )
            {
                return false;
            }
            return true;
        }
        return false;
    }

    private static List<Throwable> recordError( Throwable error, List<Throwable> errors )
    {
        if ( errors == null )
        {
            errors = new ArrayList<>();
        }
        errors.add( error );
        return errors;
    }

    private static void addSuppressed( Throwable error, List<Throwable> suppressedErrors )
    {
        if ( suppressedErrors != null )
        {
            for ( Throwable suppressedError : suppressedErrors )
            {
                if ( error != suppressedError )
                {
                    error.addSuppressed( suppressedError );
                }
            }
        }
    }
}
