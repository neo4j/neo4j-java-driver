/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ExponentialBackoff implements RetryLogic<ExponentialBackoffDecision>
{
    public static final long DEFAULT_MAX_RETRY_TIME_MS = SECONDS.toMillis( 30 );

    private static final long INITIAL_RETRY_DELAY_MS = SECONDS.toMillis( 1 );
    private static final double RETRY_DELAY_MULTIPLIER = 2.0;
    private static final double RETRY_DELAY_JITTER_FACTOR = 0.2;

    private final long maxRetryTimeMs;
    private final long initialRetryDelayMs;
    private final double multiplier;
    private final double jitterFactor;
    private final Clock clock;

    ExponentialBackoff( long maxRetryTimeMs, long initialRetryDelayMs, double multiplier, double jitterFactor,
            Clock clock )
    {
        this.maxRetryTimeMs = maxRetryTimeMs;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
        this.clock = clock;

        verifyAfterConstruction();
    }

    public static RetryLogic<RetryDecision> defaultRetryLogic()
    {
        return create( RetrySettings.DEFAULT, Clock.SYSTEM );
    }

    public static RetryLogic<RetryDecision> noRetries()
    {
        return create( new RetrySettings( 0 ), Clock.SYSTEM );
    }

    @SuppressWarnings( "unchecked" )
    public static RetryLogic<RetryDecision> create( RetrySettings settings, Clock clock )
    {
        return (RetryLogic) new ExponentialBackoff( settings.maxRetryTimeMs(), INITIAL_RETRY_DELAY_MS,
                RETRY_DELAY_MULTIPLIER, RETRY_DELAY_JITTER_FACTOR, clock );
    }

    @Override
    public ExponentialBackoffDecision apply( Throwable error, ExponentialBackoffDecision previousDecision )
    {
        Objects.requireNonNull( error );
        ExponentialBackoffDecision decision = decision( previousDecision );

        long elapsedTimeMs = clock.millis() - decision.startTimestamp();
        if ( elapsedTimeMs > maxRetryTimeMs || !canRetryOn( error ) )
        {
            return decision.stopRetrying();
        }

        long delayWithJitterMs = computeDelayWithJitter( decision.delay() );
        sleep( delayWithJitterMs );

        long nextDelayWithoutJitterMs = (long) (decision.delay() * multiplier);
        return decision.withDelay( nextDelayWithoutJitterMs );
    }

    private ExponentialBackoffDecision decision( ExponentialBackoffDecision previous )
    {
        return previous == null ? new ExponentialBackoffDecision( clock.millis(), initialRetryDelayMs ) : previous;
    }

    private long computeDelayWithJitter( long delayMs )
    {
        long jitter = (long) (delayMs * jitterFactor);
        long min = delayMs - jitter;
        long max = delayMs + jitter;
        if ( max < 0 )
        {
            // overflow detected, truncate min and max values
            min -= 1;
            max = min;
        }
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

    private static boolean canRetryOn( Throwable error )
    {
        return error instanceof SessionExpiredException ||
               error instanceof ServiceUnavailableException ||
               error instanceof TransientException;
    }
}
