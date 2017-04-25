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

import org.neo4j.driver.internal.logging.DevNullLogging;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.lang.Long.MAX_VALUE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExponentialBackoffRetryLogicTest
{
    @Test
    public void throwsForIllegalMaxRetryTime()
    {
        try
        {
            newRetryLogic( -100, 1, 1, 1, Clock.SYSTEM );
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
            newRetryLogic( 1, -100, 1, 1, Clock.SYSTEM );
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
            newRetryLogic( 1, 1, 0.42, 1, Clock.SYSTEM );
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
            newRetryLogic( 1, 1, 1, -0.42, Clock.SYSTEM );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
            assertThat( e.getMessage(), containsString( "Jitter" ) );
        }

        try
        {
            newRetryLogic( 1, 1, 1, 1.42, Clock.SYSTEM );
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
            newRetryLogic( 1, 1, 1, 1, null );
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
        int retries = 27;
        int initialDelay = 1;
        int multiplier = 3;
        int noJitter = 0;
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, noJitter, clock );

        retry( logic, retries );

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

        ExponentialBackoffRetryLogic logic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, jitterFactor, clock );

        retry( logic, retries );

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
    public void doesNotRetryWhenMaxRetryTimeExceeded() throws Exception
    {
        long retryStart = Clock.SYSTEM.millis();
        int initialDelay = 100;
        int multiplier = 2;
        long maxRetryTimeMs = 45;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( retryStart )
                .thenReturn( retryStart + maxRetryTimeMs - 5 )
                .thenReturn( retryStart + maxRetryTimeMs + 7 );

        ExponentialBackoffRetryLogic logic = newRetryLogic( maxRetryTimeMs, initialDelay, multiplier, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenThrow( error );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
        verify( workMock, times( 3 ) ).get();
    }

    @Test
    public void sleepsOnServiceUnavailableException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 42, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        ServiceUnavailableException error = serviceUnavailable();
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        assertNull( logic.retry( workMock ) );

        verify( workMock, times( 2 ) ).get();
        verify( clock ).sleep( 42 );
    }

    @Test
    public void sleepsOnSessionExpiredException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 4242, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        assertNull( logic.retry( workMock ) );

        verify( workMock, times( 2 ) ).get();
        verify( clock ).sleep( 4242 );
    }

    @Test
    public void sleepsOnTransientException() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 23, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        TransientException error = transientException();
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        assertNull( logic.retry( workMock ) );

        verify( workMock, times( 2 ) ).get();
        verify( clock ).sleep( 23 );
    }

    @Test
    public void throwsWhenUnknownError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 1, 1, 1, clock );

        Supplier<Void> workMock = newWorkMock();
        IllegalStateException error = new IllegalStateException();
        when( workMock.get() ).thenThrow( error );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    public void throwsWhenTransactionTerminatedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.Terminated", "" );
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        verify( clock, never() ).sleep( 13 );
    }

    @Test
    public void throwsWhenTransactionLockClientStoppedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" );
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        verify( clock, never() ).sleep( 13 );
    }

    @Test
    public void throwsWhenSleepInterrupted() throws Exception
    {
        Clock clock = mock( Clock.class );
        doThrow( new InterruptedException() ).when( clock ).sleep( 1 );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 1, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        when( workMock.get() ).thenThrow( serviceUnavailable() );

        try
        {
            logic.retry( workMock );
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

    @Test
    public void collectsSuppressedErrors() throws Exception
    {
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 15L ).thenReturn( 25L );
        ExponentialBackoffRetryLogic logic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        SessionExpiredException error1 = sessionExpired();
        SessionExpiredException error2 = sessionExpired();
        ServiceUnavailableException error3 = serviceUnavailable();
        TransientException error4 = transientException();
        when( workMock.get() ).thenThrow( error1, error2, error3, error4 ).thenReturn( null );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error4, e );
            Throwable[] suppressed = e.getSuppressed();
            assertEquals( 3, suppressed.length );
            assertEquals( error1, suppressed[0] );
            assertEquals( error2, suppressed[1] );
            assertEquals( error3, suppressed[2] );
        }

        verify( workMock, times( 4 ) ).get();

        verify( clock, times( 3 ) ).sleep( anyLong() );
        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
        verify( clock ).sleep( initialDelay * multiplier * multiplier );
    }

    @Test
    public void doesNotCollectSuppressedErrorsWhenSameErrorIsThrown() throws Exception
    {
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 25L );
        ExponentialBackoffRetryLogic logic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenThrow( error );

        try
        {
            logic.retry( workMock );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
            assertEquals( 0, e.getSuppressed().length );
        }

        verify( workMock, times( 3 ) ).get();

        verify( clock, times( 2 ) ).sleep( anyLong() );
        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
    }

    @Test
    public void eachRetryIsLogged()
    {
        int retries = 9;
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, clock, logging );

        retry( logic, retries );

        verify( logger, times( retries ) ).error(
                startsWith( "Transaction failed and will be retried" ),
                any( ServiceUnavailableException.class )
        );
    }

    private static void retry( ExponentialBackoffRetryLogic retryLogic, final int times )
    {
        retryLogic.retry( new Supplier<Void>()
        {
            int invoked;

            @Override
            public Void get()
            {
                if ( invoked < times )
                {
                    invoked++;
                    throw serviceUnavailable();
                }
                return null;
            }
        } );
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

    private static ExponentialBackoffRetryLogic newRetryLogic( long maxRetryTimeMs, long initialRetryDelayMs,
            double multiplier, double jitterFactor, Clock clock )
    {
        return new ExponentialBackoffRetryLogic( maxRetryTimeMs, initialRetryDelayMs, multiplier, jitterFactor, clock,
                DevNullLogging.DEV_NULL_LOGGING );
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

    @SuppressWarnings( "unchecked" )
    private static Supplier<Void> newWorkMock()
    {
        return mock( Supplier.class );
    }
}
