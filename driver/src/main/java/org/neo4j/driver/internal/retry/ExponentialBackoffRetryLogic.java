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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ExponentialBackoffRetryLogic implements RetryLogic
{
    private final static String RETRY_LOGIC_LOG_NAME = "RetryLogic";

    static final long DEFAULT_MAX_RETRY_TIME_MS = SECONDS.toMillis( 30 );

    private static final long INITIAL_RETRY_DELAY_MS = SECONDS.toMillis( 1 );
    private static final double RETRY_DELAY_MULTIPLIER = 2.0;
    private static final double RETRY_DELAY_JITTER_FACTOR = 0.2;

    private final long maxRetryTimeMs;
    private final long initialRetryDelayMs;
    private final double multiplier;
    private final double jitterFactor;
    private final Clock clock;
    private final Logger log;

    public ExponentialBackoffRetryLogic( RetrySettings settings, Clock clock, Logging logging )
    {
        this( settings.maxRetryTimeMs(), INITIAL_RETRY_DELAY_MS, RETRY_DELAY_MULTIPLIER, RETRY_DELAY_JITTER_FACTOR,
                clock, logging );
    }

    ExponentialBackoffRetryLogic( long maxRetryTimeMs, long initialRetryDelayMs, double multiplier,
            double jitterFactor, Clock clock, Logging logging )
    {
        this.maxRetryTimeMs = maxRetryTimeMs;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
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
            catch ( Throwable error )
            {
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
                        log.error( "Transaction failed and will be retried in " + delayWithJitterMs + "ms", error );

                        sleep( delayWithJitterMs );
                        nextDelayMs = (long) (nextDelayMs * multiplier);
                        errors = recordError( error, errors );
                        continue;
                    }
                }
                addSuppressed( error, errors );
                throw error;
            }
        }
    }

    private long computeDelayWithJitter( long delayMs )
    {
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

    private static boolean canRetryOn( Throwable error )
    {
        return error instanceof SessionExpiredException ||
               error instanceof ServiceUnavailableException ||
               isTransientError( error );
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
