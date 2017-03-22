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
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.lang.Long.MAX_VALUE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
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
import static org.mockito.Mockito.when;

public class ExponentialBackoffTest
{
    @Test
    public void throwsForIllegalMaxRetryTime()
    {
        try
        {
            newBackoff( -100, 1, 1, 1, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Max retry time" ) );
        }
    }

    @Test
    public void throwsForIllegalInitialRetryDelay()
    {
        try
        {
            newBackoff( 1, -100, 1, 1, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Initial retry delay" ) );
        }
    }

    @Test
    public void throwsForIllegalMultiplier()
    {
        try
        {
            newBackoff( 1, 1, 0.42, 1, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Multiplier" ) );
        }
    }

    @Test
    public void throwsForIllegalJitterFactor()
    {
        try
        {
            newBackoff( 1, 1, 1, -0.42, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Jitter" ) );
        }

        try
        {
            newBackoff( 1, 1, 1, 1.42, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Jitter" ) );
        }
    }

    @Test
    public void throwsForIllegalClock()
    {
        try
        {
            newBackoff( 1, 1, 1, 1, null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Clock" ) );
        }
    }

    @Test
    public void nextDelayCalculatedAccordingToMultiplier() throws Exception
    {
        int retries = 25;
        int initialDelay = 1;
        int multiplier = 3;
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( MAX_VALUE, initialDelay, multiplier, 0, clock );

        retry( backoff, retries );

        assertEquals( delaysWithoutJitter( initialDelay, multiplier, retries ), sleepValues( clock, retries ) );
    }

    @Test
    public void nextDelayCalculatedAccordingToJitter() throws Exception
    {
        int retries = 32;
        double jitterFactor = 0.2;
        int initialDelay = 1;
        int multiplier = 2;

        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( MAX_VALUE, initialDelay, multiplier, jitterFactor, clock );

        retry( backoff, retries );

        List<Long> sleepValues = sleepValues( clock, retries );
        List<Long> delaysWithoutJitter = delaysWithoutJitter( initialDelay, multiplier, retries );
        assertEquals( delaysWithoutJitter.size(), sleepValues.size() );

        for ( int i = 0; i < sleepValues.size(); i++ )
        {
            double sleepValue = sleepValues.get( i ).doubleValue();
            long delayWithoutJitter = delaysWithoutJitter.get( i );
            double jitter = delayWithoutJitter * jitterFactor;

            assertThat( sleepValue, closeTo( delayWithoutJitter, jitter ) );
        }
    }

    @Test
    public void acceptsNullPreviousDecision()
    {
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 1, Clock.SYSTEM );

        ExponentialBackoffDecision decision = backoff.apply( serviceUnavailable(), null );

        assertNotNull( decision );
        assertTrue( decision.shouldRetry() );
    }

    @Test
    public void doesNothingWhenMaxRetryTimeExceeded() throws Exception
    {
        long retryStart = Clock.SYSTEM.millis();
        long maxRetryTimeMs = 45;
        long nextMillis = retryStart + maxRetryTimeMs + 1;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( nextMillis );

        ExponentialBackoff backoff = newBackoff( maxRetryTimeMs, 1, 1, 1, clock );
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( retryStart, 5 );

        ExponentialBackoffDecision newDecision = backoff.apply( serviceUnavailable(), decision );

        assertFalse( newDecision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void doesNothingWhenUnknownError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 1, clock );

        ExponentialBackoffDecision decision = backoff.apply( new IllegalStateException(), null );

        assertFalse( decision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void sleepsOnServiceUnavailableException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        ExponentialBackoffDecision decision = backoff.apply( serviceUnavailable(), null );

        assertTrue( decision.shouldRetry() );
        verify( clock ).sleep( 1 );
    }

    @Test
    public void sleepsOnSessionExpiredException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        ExponentialBackoffDecision decision = backoff.apply( sessionExpired(), null );

        assertTrue( decision.shouldRetry() );
        verify( clock ).sleep( 1 );
    }

    @Test
    public void sleepsOnTransientException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        ExponentialBackoffDecision decision = backoff.apply( transientException(), null );

        assertTrue( decision.shouldRetry() );
        verify( clock ).sleep( 1 );
    }

    @Test
    public void doesNothingWhenTransactionTerminatedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        TransientException exception = new TransientException( "Neo.TransientError.Transaction.Terminated", "" );
        ExponentialBackoffDecision decision = backoff.apply( exception, null );

        assertFalse( decision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void doesNothingWhenTransactionLockClientStoppedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        TransientException exception = new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" );
        ExponentialBackoffDecision decision = backoff.apply( exception, null );

        assertFalse( decision.shouldRetry() );
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void throwsWhenSleepInterrupted() throws Exception
    {
        Clock clock = mock( Clock.class );
        doThrow( new InterruptedException() ).when( clock ).sleep( 1 );
        ExponentialBackoff backoff = newBackoff( 1, 1, 1, 0, clock );

        try
        {
            backoff.apply( serviceUnavailable(), null );
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

    private static void retry( ExponentialBackoff backoff, int times )
    {
        ExponentialBackoffDecision decision = null;
        for ( int i = 0; i < times; i++ )
        {
            decision = backoff.apply( serviceUnavailable(), decision );
        }
    }

    private static List<Long> delaysWithoutJitter( long initialDelay, double multiplier, int count )
    {
        List<Long> values = new ArrayList<>();
        long delay = initialDelay;
        do
        {
            values.add( delay );
            delay *= multiplier;
        }
        while ( --count > 0 );
        return values;
    }

    private static List<Long> sleepValues( Clock clockMock, int expectedCount ) throws InterruptedException
    {
        ArgumentCaptor<Long> captor = ArgumentCaptor.forClass( long.class );
        verify( clockMock, times( expectedCount ) ).sleep( captor.capture() );
        return captor.getAllValues();
    }

    private static ExponentialBackoff newBackoff( long maxRetryTimeMs, long initialRetryDelayMs,
            double multiplier, double jitterFactor, Clock clock )
    {
        return new ExponentialBackoff( maxRetryTimeMs, initialRetryDelayMs, multiplier, jitterFactor, clock );
    }

    private static ServiceUnavailableException serviceUnavailable()
    {
        return new ServiceUnavailableException( "" );
    }

    private static SessionExpiredException sessionExpired()
    {
        return new SessionExpiredException( "" );
    }

    private static TransientException transientException()
    {
        return new TransientException( "", "" );
    }
}
