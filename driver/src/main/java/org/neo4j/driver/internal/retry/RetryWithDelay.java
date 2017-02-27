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

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

public class RetryWithDelay implements RetryLogic<RetryWithDelayDecision>
{
    private final int maxAttempts;
    private final long delayMs;
    private final Clock clock;

    RetryWithDelay( int maxAttempts, long delayMs, Clock clock )
    {
        this.maxAttempts = maxAttempts;
        this.delayMs = delayMs;
        this.clock = clock;
    }

    public static RetryLogic<RetryDecision> defaultRetryLogic()
    {
        return create( RetrySettings.DEFAULT, Clock.SYSTEM );
    }

    public static RetryLogic<RetryDecision> noRetries()
    {
        return create( new RetrySettings( 0, 0 ), Clock.SYSTEM );
    }

    @SuppressWarnings( "unchecked" )
    public static RetryLogic<RetryDecision> create( RetrySettings settings, Clock clock )
    {
        return (RetryLogic) new RetryWithDelay( settings.maxAttempts(), settings.delayMs(), clock );
    }

    @Override
    public RetryWithDelayDecision apply( Throwable error, RetryWithDelayDecision previousDecision )
    {
        Objects.requireNonNull( error );
        RetryWithDelayDecision decision = decision( previousDecision );

        if ( decision.attempt() >= maxAttempts || !databaseUnavailable( error ) )
        {
            return decision.stopRetrying();
        }

        sleep();
        return decision.incrementAttempt();
    }

    private void sleep()
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

    private static boolean databaseUnavailable( Throwable error )
    {
        return error instanceof SessionExpiredException || error instanceof ServiceUnavailableException;
    }

    private static RetryWithDelayDecision decision( RetryWithDelayDecision previousDecision )
    {
        return previousDecision == null ? new RetryWithDelayDecision() : previousDecision;
    }
}
