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

import org.junit.Test;

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryWithDelayTest
{
    @Test
    public void throwsWhenErrorIsNull()
    {
        RetryWithDelay retryLogic = newRetryWithDelay( 1, 1, Clock.SYSTEM );

        try
        {
            retryLogic.apply( null, null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void acceptsNullPreviousDecision()
    {
        RetryWithDelay retryLogic = newRetryWithDelay( 5, 1, mock( Clock.class ) );

        RetryWithDelayDecision decision = retryLogic.apply( serviceUnavailable(), null );

        assertEquals( 2, decision.attempt() );
        assertTrue( decision.shouldRetry() );
    }

    @Test
    public void doesNothingWhenMaxAttemptsExceeded() throws Exception
    {
        int maxAttempts = 5;
        Clock clock = mock( Clock.class );
        RetryWithDelay retryLogic = newRetryWithDelay( maxAttempts, 1, clock );
        RetryWithDelayDecision decision = new RetryWithDelayDecision( maxAttempts + 1 );

        RetryWithDelayDecision newDecision = retryLogic.apply( serviceUnavailable(), decision );

        assertEquals( maxAttempts + 1, newDecision.attempt() );
        assertFalse( newDecision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void doesNothingWhenUnknownError() throws Exception
    {
        Clock clock = mock( Clock.class );
        RetryWithDelay retryLogic = newRetryWithDelay( 42, 1, clock );
        RetryWithDelayDecision decision = new RetryWithDelayDecision();

        RetryWithDelayDecision newDecision = retryLogic.apply( new IllegalStateException(), decision );

        assertEquals( 1, newDecision.attempt() );
        assertFalse( newDecision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void incrementsAttemptCount()
    {
        RetryWithDelay retryLogic = newRetryWithDelay( 5, 1, mock( Clock.class ) );
        RetryWithDelayDecision decision1 = new RetryWithDelayDecision( 0 );

        RetryWithDelayDecision decision2 = retryLogic.apply( sessionExpired(), decision1 );
        assertEquals( 1, decision2.attempt() );

        RetryWithDelayDecision decision3 = retryLogic.apply( serviceUnavailable(), decision2 );
        assertEquals( 2, decision3.attempt() );

        RetryWithDelayDecision decision4 = retryLogic.apply( sessionExpired(), decision3 );
        assertEquals( 3, decision4.attempt() );

        RetryWithDelayDecision decision5 = retryLogic.apply( serviceUnavailable(), decision4 );
        assertEquals( 4, decision5.attempt() );
    }

    @Test
    public void sleepsOnEveryAttempt() throws Exception
    {
        Clock clock = mock( Clock.class );
        RetryWithDelay retryLogic = newRetryWithDelay( 5, 42, clock );
        RetryWithDelayDecision decision1 = new RetryWithDelayDecision( 0 );

        RetryWithDelayDecision decision2 = retryLogic.apply( serviceUnavailable(), decision1 );
        verify( clock ).sleep( 42 );

        RetryWithDelayDecision decision3 = retryLogic.apply( sessionExpired(), decision2 );
        verify( clock, times( 2 ) ).sleep( 42 );

        RetryWithDelayDecision decision4 = retryLogic.apply( serviceUnavailable(), decision3 );
        verify( clock, times( 3 ) ).sleep( 42 );

        assertNotNull( retryLogic.apply( sessionExpired(), decision4 ) );
        verify( clock, times( 4 ) ).sleep( 42 );
    }

    @Test
    public void throwsWhenSleepInterrupted() throws Exception
    {
        Clock clock = mock( Clock.class );
        doThrow( new InterruptedException() ).when( clock ).sleep( 42 );
        RetryWithDelay retryLogic = newRetryWithDelay( 5, 42, clock );

        try
        {
            retryLogic.apply( sessionExpired(), null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
            assertThat( e.getCause(), instanceOf( InterruptedException.class ) );
        }
        finally
        {
            // Clear the interruption flag so all subsequent tests do not see this thread as interrupted
            Thread.interrupted();
        }
    }

    private static RetryWithDelay newRetryWithDelay( int maxAttempts, long delayMs, Clock clock )
    {
        return new RetryWithDelay( maxAttempts, delayMs, clock );
    }

    private static ServiceUnavailableException serviceUnavailable()
    {
        return new ServiceUnavailableException( "" );
    }

    private static SessionExpiredException sessionExpired()
    {
        return new SessionExpiredException( "" );
    }
}
