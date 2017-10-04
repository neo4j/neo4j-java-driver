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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.async.Futures;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.internal.util.TrackingEventExecutor;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.Futures.failedFuture;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class ExponentialBackoffRetryLogicTest
{
    private final TrackingEventExecutor eventExecutor = new TrackingEventExecutor();

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
    public void nextDelayCalculatedAccordingToMultiplier()
    {
        String result = "The Result";
        int retries = 14;
        int initialDelay = 1;
        int multiplier = 2;
        int noJitter = 0;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, noJitter,
                Clock.SYSTEM );

        CompletionStage<Object> future = retry( retryLogic, retries, result );

        assertEquals( result, Futures.getBlocking( future ) );
        assertEquals( delaysWithoutJitter( initialDelay, multiplier, retries ), eventExecutor.scheduleDelays() );
    }

    @Test
    public void nextDelayCalculatedAccordingToJitter()
    {
        String result = "The Result";
        int retries = 24;
        double jitterFactor = 0.2;
        int initialDelay = 1;
        int multiplier = 2;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, jitterFactor,
                mock( Clock.class ) );

        CompletionStage<Object> future = retry( retryLogic, retries, result );
        assertEquals( result, Futures.getBlocking( future ) );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        List<Long> delaysWithoutJitter = delaysWithoutJitter( initialDelay, multiplier, retries );

        assertDelaysApproximatelyEqual( delaysWithoutJitter, scheduleDelays, jitterFactor );
    }

    @Test
    public void doesNotRetryWhenMaxRetryTimeExceeded()
    {
        long retryStart = Clock.SYSTEM.millis();
        int initialDelay = 100;
        int multiplier = 2;
        long maxRetryTimeMs = 45;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( retryStart )
                .thenReturn( retryStart + maxRetryTimeMs - 5 )
                .thenReturn( retryStart + maxRetryTimeMs + 7 );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( maxRetryTimeMs, initialDelay, multiplier, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        CompletionStage<Object> future = retryLogic.retry( workMock );

        try
        {
            await( future );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );

        verify( workMock, times( 3 ) ).get();
    }

    @Test
    public void schedulesRetryOnServiceUnavailableException()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 42, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retry( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 42, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    public void schedulesRetryOnSessionExpiredException()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 4242, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retry( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 4242, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    public void schedulesRetryOnTransientException()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 23, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = transientException();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retry( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 23, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    public void doesNotRetryOnUnknownError()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 1, 1, 1, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        IllegalStateException error = new IllegalStateException();
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        try
        {
            await( retryLogic.retry( workMock ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    public void doesNotRetryOnTransactionTerminatedError()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.Terminated", "" );
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        try
        {
            await( retryLogic.retry( workMock ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    public void doesNotRetryOnTransactionLockClientStoppedError()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 15, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" );
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        try
        {
            await( retryLogic.retry( workMock ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    public void collectsSuppressedErrors()
    {
        String result = "The Result";
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 15L ).thenReturn( 25L );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error1 = sessionExpired();
        SessionExpiredException error2 = sessionExpired();
        ServiceUnavailableException error3 = serviceUnavailable();
        TransientException error4 = transientException();

        when( workMock.get() ).thenReturn( failedFuture( error1 ) )
                .thenReturn( failedFuture( error2 ) )
                .thenReturn( failedFuture( error3 ) )
                .thenReturn( failedFuture( error4 ) )
                .thenReturn( completedFuture( result ) );

        try
        {
            Futures.getBlocking( retryLogic.retry( workMock ) );
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

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 3, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
        assertEquals( initialDelay * multiplier * multiplier, scheduleDelays.get( 2 ).intValue() );
    }

    @Test
    public void doesNotCollectSuppressedErrorsWhenSameErrorIsThrown()
    {
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 25L );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        try
        {
            Futures.getBlocking( retryLogic.retry( workMock ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
            assertEquals( 0, e.getSuppressed().length );
        }

        verify( workMock, times( 3 ) ).get();

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
    }

    @Test
    public void eachRetryIsLogged()
    {
        String result = "The Result";
        int retries = 9;
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                clock, logging );

        assertEquals( result, await( retry( logic, retries, result ) ) );

        verify( logger, times( retries ) ).warn(
                startsWith( "Transaction failed and is scheduled to retry" ),
                any( ServiceUnavailableException.class )
        );
    }

    private CompletionStage<Object> retry( ExponentialBackoffRetryLogic retryLogic, final int times,
            final Object result )
    {
        return retryLogic.retry( new Supplier<CompletionStage<Object>>()
        {
            int invoked;

            @Override
            public CompletionStage<Object> get()
            {
                if ( invoked < times )
                {
                    invoked++;
                    return failedFuture( serviceUnavailable() );
                }
                return completedFuture( result );
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

    private ExponentialBackoffRetryLogic newRetryLogic( long maxRetryTimeMs, long initialRetryDelayMs,
            double multiplier, double jitterFactor, Clock clock )
    {
        return new ExponentialBackoffRetryLogic( maxRetryTimeMs, initialRetryDelayMs, multiplier, jitterFactor,
                eventExecutor, clock, DEV_NULL_LOGGING );
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
    private static <T> Supplier<T> newWorkMock()
    {
        return mock( Supplier.class );
    }

    private static void assertDelaysApproximatelyEqual( List<Long> expectedDelays, List<Long> actualDelays,
            double delta )
    {
        assertEquals( expectedDelays.size(), actualDelays.size() );

        for ( int i = 0; i < actualDelays.size(); i++ )
        {
            double actualValue = actualDelays.get( i ).doubleValue();
            long expectedValue = expectedDelays.get( i );

            assertThat( actualValue, closeTo( expectedValue, expectedValue * delta ) );
        }
    }
}
