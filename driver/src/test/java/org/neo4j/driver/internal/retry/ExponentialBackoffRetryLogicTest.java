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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.util.TestUtil.await;

class ExponentialBackoffRetryLogicTest
{
    private final ImmediateSchedulingEventExecutor eventExecutor = new ImmediateSchedulingEventExecutor();

    @Test
    void throwsForIllegalMaxRetryTime()
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( -100, 1, 1, 1, Clock.SYSTEM ) );
        assertThat( error.getMessage(), containsString( "Max retry time" ) );
    }

    @Test
    void throwsForIllegalInitialRetryDelay()
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( 1, -100, 1, 1, Clock.SYSTEM ) );
        assertThat( error.getMessage(), containsString( "Initial retry delay" ) );
    }

    @Test
    void throwsForIllegalMultiplier()
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( 1, 1, 0.42, 1, Clock.SYSTEM ) );
        assertThat( error.getMessage(), containsString( "Multiplier" ) );
    }

    @Test
    void throwsForIllegalJitterFactor()
    {
        IllegalArgumentException error1 = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( 1, 1, 1, -0.42, Clock.SYSTEM ) );
        assertThat( error1.getMessage(), containsString( "Jitter" ) );

        IllegalArgumentException error2 = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( 1, 1, 1, 1.42, Clock.SYSTEM ) );
        assertThat( error2.getMessage(), containsString( "Jitter" ) );
    }

    @Test
    void throwsForIllegalClock()
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> newRetryLogic( 1, 1, 1, 1, null ) );
        assertThat( error.getMessage(), containsString( "Clock" ) );
    }

    @Test
    void nextDelayCalculatedAccordingToMultiplier() throws Exception
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
    void nextDelayCalculatedAccordingToMultiplierAsync()
    {
        String result = "The Result";
        int retries = 14;
        int initialDelay = 1;
        int multiplier = 2;
        int noJitter = 0;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, noJitter,
                Clock.SYSTEM );

        CompletionStage<Object> future = retryAsync( retryLogic, retries, result );

        assertEquals( result, await( future ) );
        assertEquals( delaysWithoutJitter( initialDelay, multiplier, retries ), eventExecutor.scheduleDelays() );
    }

    @Test
    void nextDelayCalculatedAccordingToMultiplierRx()
    {
        String result = "The Result";
        int retries = 14;
        int initialDelay = 1;
        int multiplier = 2;
        int noJitter = 0;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, noJitter,
                Clock.SYSTEM );

        Mono<Object> single = Flux.from( retryRx( retryLogic, retries, result ) ).single();

        assertEquals( result, await( single ) );
        assertEquals( delaysWithoutJitter( initialDelay, multiplier, retries ), eventExecutor.scheduleDelays() );
    }

    @Test
    void nextDelayCalculatedAccordingToJitter() throws Exception
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

        assertDelaysApproximatelyEqual( delaysWithoutJitter, sleepValues, jitterFactor );
    }

    @Test
    void nextDelayCalculatedAccordingToJitterAsync()
    {
        String result = "The Result";
        int retries = 24;
        double jitterFactor = 0.2;
        int initialDelay = 1;
        int multiplier = 2;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, jitterFactor,
                mock( Clock.class ) );

        CompletionStage<Object> future = retryAsync( retryLogic, retries, result );
        assertEquals( result, await( future ) );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        List<Long> delaysWithoutJitter = delaysWithoutJitter( initialDelay, multiplier, retries );

        assertDelaysApproximatelyEqual( delaysWithoutJitter, scheduleDelays, jitterFactor );
    }

    @Test
    void nextDelayCalculatedAccordingToJitterRx()
    {
        String result = "The Result";
        int retries = 24;
        double jitterFactor = 0.2;
        int initialDelay = 1;
        int multiplier = 2;

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( MAX_VALUE, initialDelay, multiplier, jitterFactor,
                mock( Clock.class ) );

        Mono<Object> single = Flux.from( retryRx( retryLogic, retries, result ) ).single();
        assertEquals( result, await( single ) );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        List<Long> delaysWithoutJitter = delaysWithoutJitter( initialDelay, multiplier, retries );

        assertDelaysApproximatelyEqual( delaysWithoutJitter, scheduleDelays, jitterFactor );
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceeded() throws Exception
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

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error, e );

        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
        verify( workMock, times( 3 ) ).get();
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceededAsync()
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

        CompletionStage<Object> future = retryLogic.retryAsync( workMock );

        Exception e = assertThrows( Exception.class, () -> await( future ) );
        assertEquals( error, e );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );

        verify( workMock, times( 3 ) ).get();
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceededRx()
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

        SessionExpiredException error = sessionExpired();
        AtomicInteger executionCount = new AtomicInteger();
        Publisher<Object> publisher = retryLogic.retryRx( Mono.error( error ).doOnTerminate( executionCount::getAndIncrement ) );

        Exception e = assertThrows( Exception.class, () -> await( publisher ) );
        assertEquals( error, e );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );

        assertThat( executionCount.get(), equalTo( 3 ) );
    }

    @Test
    void sleepsOnServiceUnavailableException() throws Exception
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
    void schedulesRetryOnServiceUnavailableExceptionAsync()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 42, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        ServiceUnavailableException error = serviceUnavailable();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retryAsync( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 42, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    void sleepsOnSessionExpiredException() throws Exception
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
    void schedulesRetryOnSessionExpiredExceptionAsync()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 4242, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        SessionExpiredException error = sessionExpired();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retryAsync( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 4242, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    void sleepsOnTransientException() throws Exception
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
    void schedulesRetryOnTransientExceptionAsync()
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 23, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = transientException();
        when( workMock.get() ).thenReturn( failedFuture( error ) ).thenReturn( completedFuture( result ) );

        assertEquals( result, await( retryLogic.retryAsync( workMock ) ) );

        verify( workMock, times( 2 ) ).get();
        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 23, scheduleDelays.get( 0 ).intValue() );
    }

    @Test
    void throwsWhenUnknownError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 1, 1, 1, clock );

        Supplier<Void> workMock = newWorkMock();
        IllegalStateException error = new IllegalStateException();
        when( workMock.get() ).thenThrow( error );

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error, e );

        verify( workMock ).get();
        verify( clock, never() ).sleep( anyLong() );
    }

    @Test
    void doesNotRetryOnUnknownErrorAsync()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 1, 1, 1, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        IllegalStateException error = new IllegalStateException();
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> await( retryLogic.retryAsync( workMock ) ) );
        assertEquals( error, e );

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    void throwsWhenTransactionTerminatedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.Terminated", "" );
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error, e );

        verify( workMock ).get();
        verify( clock, never() ).sleep( 13 );
    }

    @Test
    void doesNotRetryOnTransactionTerminatedErrorAsync()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.Terminated", "" );
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> await( retryLogic.retryAsync( workMock ) ) );
        assertEquals( error, e );

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    void throwsWhenTransactionLockClientStoppedError() throws Exception
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 13, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" );
        when( workMock.get() ).thenThrow( error ).thenReturn( null );

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error, e );

        verify( workMock ).get();
        verify( clock, never() ).sleep( 13 );
    }

    @Test
    void doesNotRetryOnTransactionLockClientStoppedErrorAsync()
    {
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 15, 1, 0, clock );

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        TransientException error = new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" );
        when( workMock.get() ).thenReturn( failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> await( retryLogic.retryAsync( workMock ) ) );
        assertEquals( error, e );

        verify( workMock ).get();
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @ParameterizedTest
    @MethodSource( "canBeRetriedErrors" )
    void schedulesRetryOnErrorRx( Exception error )
    {
        String result = "The Result";
        Clock clock = mock( Clock.class );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 4242, 1, 0, clock );

        Publisher<String> publisher = createMono( result, error );
        Mono<String> single = Flux.from( retryLogic.retryRx( publisher ) ).single();

        assertEquals( result, await( single ) );

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 1, scheduleDelays.size() );
        assertEquals( 4242, scheduleDelays.get( 0 ).intValue() );
    }

    @ParameterizedTest
    @MethodSource( "cannotBeRetriedErrors" )
    void scheduleNoRetryOnErrorRx( Exception error )
    {
        Clock clock = mock( Clock.class );
        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( 1, 10, 1, 1, clock );

        Mono<Object> single = Flux.from( retryLogic.retryRx( Mono.error( error ) ) ).single();

        Exception e = assertThrows( Exception.class, () -> await( single ) );
        assertEquals( error, e );

        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    @Test
    void throwsWhenSleepInterrupted() throws Exception
    {
        Clock clock = mock( Clock.class );
        doThrow( new InterruptedException() ).when( clock ).sleep( 1 );
        ExponentialBackoffRetryLogic logic = newRetryLogic( 1, 1, 1, 0, clock );

        Supplier<Void> workMock = newWorkMock();
        when( workMock.get() ).thenThrow( serviceUnavailable() );

        try
        {
            IllegalStateException e = assertThrows( IllegalStateException.class, () -> logic.retry( workMock ) );
            assertThat( e.getCause(), instanceOf( InterruptedException.class ) );
        }
        finally
        {
            // Clear the interruption flag so all subsequent tests do not see this thread as interrupted
            Thread.interrupted();
        }
    }

    @Test
    void collectsSuppressedErrors() throws Exception
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

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error4, e );
        Throwable[] suppressed = e.getSuppressed();
        assertEquals( 3, suppressed.length );
        assertEquals( error1, suppressed[0] );
        assertEquals( error2, suppressed[1] );
        assertEquals( error3, suppressed[2] );

        verify( workMock, times( 4 ) ).get();

        verify( clock, times( 3 ) ).sleep( anyLong() );
        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
        verify( clock ).sleep( initialDelay * multiplier * multiplier );
    }

    @Test
    void collectsSuppressedErrorsAsync()
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

        Exception e = assertThrows( Exception.class, () -> await( retryLogic.retryAsync( workMock ) ) );
        assertEquals( error4, e );
        Throwable[] suppressed = e.getSuppressed();
        assertEquals( 3, suppressed.length );
        assertEquals( error1, suppressed[0] );
        assertEquals( error2, suppressed[1] );
        assertEquals( error3, suppressed[2] );

        verify( workMock, times( 4 ) ).get();

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 3, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
        assertEquals( initialDelay * multiplier * multiplier, scheduleDelays.get( 2 ).intValue() );
    }

    @Test
    void collectsSuppressedErrorsRx() throws Exception
    {
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 15L ).thenReturn( 25L );
        ExponentialBackoffRetryLogic logic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        SessionExpiredException error1 = sessionExpired();
        SessionExpiredException error2 = sessionExpired();
        ServiceUnavailableException error3 = serviceUnavailable();
        TransientException error4 = transientException();
        Mono<String> mono = createMono( "A result", error1, error2, error3, error4 );

        StepVerifier.create( logic.retryRx( mono ) ).expectErrorSatisfies( e -> {
            assertEquals( error4, e );
            Throwable[] suppressed = e.getSuppressed();
            assertEquals( 3, suppressed.length );
            assertEquals( error1, suppressed[0] );
            assertEquals( error2, suppressed[1] );
            assertEquals( error3, suppressed[2] );
        } ).verify();

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 3, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
        assertEquals( initialDelay * multiplier * multiplier, scheduleDelays.get( 2 ).intValue() );    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrown() throws Exception
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

        Exception e = assertThrows( Exception.class, () -> logic.retry( workMock ) );
        assertEquals( error, e );
        assertEquals( 0, e.getSuppressed().length );

        verify( workMock, times( 3 ) ).get();

        verify( clock, times( 2 ) ).sleep( anyLong() );
        verify( clock ).sleep( initialDelay );
        verify( clock ).sleep( initialDelay * multiplier );
    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrownAsync()
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

        Exception e = assertThrows( Exception.class, () -> await( retryLogic.retryAsync( workMock ) ) );
        assertEquals( error, e );
        assertEquals( 0, e.getSuppressed().length );

        verify( workMock, times( 3 ) ).get();

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrownRx()
    {
        long maxRetryTime = 20;
        int initialDelay = 15;
        int multiplier = 2;
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L ).thenReturn( 10L ).thenReturn( 25L );

        ExponentialBackoffRetryLogic retryLogic = newRetryLogic( maxRetryTime, initialDelay, multiplier, 0, clock );

        SessionExpiredException error = sessionExpired();
        StepVerifier.create( retryLogic.retryRx( Mono.error( error ) ) ).expectErrorSatisfies( e -> assertEquals( error, e ) ).verify();

        List<Long> scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals( 2, scheduleDelays.size() );
        assertEquals( initialDelay, scheduleDelays.get( 0 ).intValue() );
        assertEquals( initialDelay * multiplier, scheduleDelays.get( 1 ).intValue() );
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCause()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        String result = logic.retry( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw clientExceptionWithValidTerminationCause();
            }
            return "Done";
        } );

        assertEquals( "Done", result );
    }

    @Test
    void doesNotRetryOnRandomClientException()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        ClientException exception = Assertions.assertThrows( ClientException.class, () -> logic.retry( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw randomClientException();
            }
            return "Done";
        } ) );

        assertEquals( "Meeh", exception.getMessage() );
    }

    @Test
    void eachRetryIsLogged()
    {
        int retries = 9;
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                clock, logging );

        retry( logic, retries );

        verify( logger, times( retries ) ).warn(
                startsWith( "Transaction failed and will be retried" ),
                any( ServiceUnavailableException.class )
        );
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCauseAsync()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        String result = await( logic.retryAsync( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw clientExceptionWithValidTerminationCause();
            }
            return CompletableFuture.completedFuture( "Done" );
        } ) );

        assertEquals( "Done", result );
    }

    @Test
    void doesNotRetryOnRandomClientExceptionAsync()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        ClientException exception = Assertions.assertThrows( ClientException.class, () -> await( logic.retryAsync( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw randomClientException();
            }
            return CompletableFuture.completedFuture( "Done" );
        } ) ) );

        assertEquals( "Meeh", exception.getMessage() );
    }

    @Test
    void eachRetryIsLoggedAsync()
    {
        String result = "The Result";
        int retries = 9;
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                clock, logging );

        assertEquals( result, await( retryAsync( logic, retries, result ) ) );

        verify( logger, times( retries ) ).warn(
                startsWith( "Async transaction failed and is scheduled to retry" ),
                any( ServiceUnavailableException.class )
        );
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCauseRx()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        String result = await( Mono.from( logic.retryRx( Mono.fromSupplier( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw clientExceptionWithValidTerminationCause();
            }
            return "Done";
        } ) ) ) );

        assertEquals( "Done", result );
    }

    @Test
    void doesNotRetryOnRandomClientExceptionRx()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor, clock, logging );

        AtomicBoolean exceptionThrown = new AtomicBoolean( false );
        ClientException exception = Assertions.assertThrows( ClientException.class, () -> await( Mono.from( logic.retryRx( Mono.fromSupplier( () ->
        {
            if ( exceptionThrown.compareAndSet( false, true ) )
            {
                throw randomClientException();
            }
            return "Done";
        } ) ) ) ) );

        assertEquals( "Meeh", exception.getMessage() );
    }

    @Test
    void eachRetryIsLoggedRx()
    {
        String result = "The Result";
        int retries = 9;
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                clock, logging );

        assertEquals( result, await( Flux.from( retryRx( logic, retries, result ) ).single() ) );

        verify( logger, times( retries ) ).warn(
                startsWith( "Reactive transaction failed and is scheduled to retry" ),
                any( ServiceUnavailableException.class )
        );
    }

    @Test
    void nothingIsLoggedOnFatalFailure()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                mock( Clock.class ), logging );

        RuntimeException error = assertThrows( RuntimeException.class, () ->
                logic.retry( () ->
                {
                    throw new RuntimeException( "Fatal blocking" );
                } ) );
        assertEquals( "Fatal blocking", error.getMessage() );
        verifyZeroInteractions( logger );
    }

    @Test
    void nothingIsLoggedOnFatalFailureAsync()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                mock( Clock.class ), logging );

        RuntimeException error = assertThrows( RuntimeException.class, () ->
                await( logic.retryAsync( () -> failedFuture( new RuntimeException( "Fatal async" ) ) ) ) );

        assertEquals( "Fatal async", error.getMessage() );
        verifyZeroInteractions( logger );
    }

    @Test
    void nothingIsLoggedOnFatalFailureRx()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        ExponentialBackoffRetryLogic logic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, eventExecutor,
                mock( Clock.class ), logging );

        Publisher<Object> retryRx = logic.retryRx( Mono.error( new RuntimeException( "Fatal rx" ) ) );
        RuntimeException error = assertThrows( RuntimeException.class, () -> await( retryRx ) );

        assertEquals( "Fatal rx", error.getMessage() );
        verifyZeroInteractions( logger );
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailure()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        RetrySettings settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic( settings, eventExecutor, clock, logging );

        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () ->
                logic.retry( new Supplier<Long>()
                {
                    boolean invoked;

                    @Override
                    public Long get()
                    {
                        // work that always fails and moves clock forward after the first failure
                        if ( invoked )
                        {
                            // move clock forward to stop retries
                            when( clock.millis() ).thenReturn( settings.maxRetryTimeMs() + 42 );
                        }
                        else
                        {
                            invoked = true;
                        }
                        throw new ServiceUnavailableException( "Error" );
                    }
                } ) );
        assertEquals( "Error", error.getMessage() );
        verify( logger ).warn(
                startsWith( "Transaction failed and will be retried" ),
                any( ServiceUnavailableException.class )
        );
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailureAsync()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        RetrySettings settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic( settings, eventExecutor, clock, logging );

        SessionExpiredException error = assertThrows( SessionExpiredException.class, () ->
                await( logic.retryAsync( new Supplier<CompletionStage<Void>>()
                {
                    volatile boolean invoked;

                    @Override
                    public CompletionStage<Void> get()
                    {
                        // work that always returns failed future and moves clock forward after the first failure
                        if ( invoked )
                        {
                            // move clock forward to stop retries
                            when( clock.millis() ).thenReturn( settings.maxRetryTimeMs() + 42 );
                        }
                        else
                        {
                            invoked = true;
                        }
                        return failedFuture( new SessionExpiredException( "Session no longer valid" ) );
                    }
                } ) ) );
        assertEquals( "Session no longer valid", error.getMessage() );
        verify( logger ).warn(
                startsWith( "Async transaction failed and is scheduled to retry" ),
                any( SessionExpiredException.class )
        );
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailureRx()
    {
        Clock clock = mock( Clock.class );
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        RetrySettings settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic( settings, eventExecutor, clock, logging );

        AtomicBoolean invoked = new AtomicBoolean( false );
        SessionExpiredException error = assertThrows( SessionExpiredException.class, () ->
                await( logic.retryRx( Mono.create( e -> {
                    if ( invoked.get() )
                    {
                        // move clock forward to stop retries
                        when( clock.millis() ).thenReturn( settings.maxRetryTimeMs() + 42 );
                    }
                    else
                    {
                        invoked.set( true );
                    }
                    e.error( new SessionExpiredException( "Session no longer valid" ) );
                } ) ) ) );
        assertEquals( "Session no longer valid", error.getMessage() );
        verify( logger ).warn(
                startsWith( "Reactive transaction failed and is scheduled to retry" ),
                any( SessionExpiredException.class )
        );
    }

    @Test
    void shouldRetryWithBackOffRx()
    {
        Exception exception = new TransientException( "Unknown", "Retry this error." );
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 100L, 200L, 400L, 800L );
        ExponentialBackoffRetryLogic retryLogic = new ExponentialBackoffRetryLogic( 500, 100, 2, 0,
                eventExecutor, clock, DEV_NULL_LOGGING );

        Flux<Integer> source = Flux.concat( Flux.range( 0, 2 ), Flux.error( exception ) );
        Flux<Integer> retriedSource = Flux.from( retryLogic.retryRx( source ) );
        StepVerifier.create( retriedSource )
                .expectNext( 0, 1 ) // first run
                .expectNext( 0, 1, 0, 1, 0, 1, 0, 1 ) //4 retry attempts
                .verifyErrorSatisfies( e -> assertThat( e, equalTo( exception ) ) );

        List<Long> delays = eventExecutor.scheduleDelays();
        assertThat( delays.size(), equalTo( 4 ) );
        assertThat( delays, contains( 100L, 200L, 400L, 800L ) );
    }

    @Test
    void shouldRetryWithRandomBackOffRx()
    {
        Exception exception = new TransientException( "Unknown", "Retry this error." );
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 100L, 200L, 400L, 800L );
        ExponentialBackoffRetryLogic retryLogic = new ExponentialBackoffRetryLogic( 500, 100, 2, 0.1,
                eventExecutor, clock, DEV_NULL_LOGGING );

        Flux<Integer> source = Flux.concat( Flux.range( 0, 2 ), Flux.error( exception ) );
        Flux<Integer> retriedSource = Flux.from( retryLogic.retryRx( source ) );
        StepVerifier.create( retriedSource )
                .expectNext( 0, 1 ) // first run
                .expectNext( 0, 1, 0, 1, 0, 1, 0, 1 ) // 4 retry attempts
                .verifyErrorSatisfies( e -> assertThat( e, equalTo( exception ) ) );

        List<Long> delays = eventExecutor.scheduleDelays();
        assertThat( delays.size(), equalTo( 4 ) );
        assertThat( delays.get( 0 ), allOf( greaterThanOrEqualTo( 90L ), lessThanOrEqualTo( 110L ) ) );
        assertThat( delays.get( 1 ), allOf( greaterThanOrEqualTo( 180L ), lessThanOrEqualTo( 220L ) ) );
        assertThat( delays.get( 2 ), allOf( greaterThanOrEqualTo( 260L ), lessThanOrEqualTo( 440L ) ) );
        assertThat( delays.get( 3 ), allOf( greaterThanOrEqualTo( 720L ), lessThanOrEqualTo( 880L ) ) );
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

    private CompletionStage<Object> retryAsync( ExponentialBackoffRetryLogic retryLogic, int times, Object result )
    {
        return retryLogic.retryAsync( new Supplier<CompletionStage<Object>>()
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

    private Publisher<Object> retryRx( ExponentialBackoffRetryLogic retryLogic, int times, Object result )
    {
        AtomicInteger invoked = new AtomicInteger();
        return retryLogic.retryRx( Mono.create( e -> {
            if ( invoked.get() < times )
            {
                invoked.getAndIncrement();
                e.error( serviceUnavailable() );
            }
            else
            {
                e.success( result );
            }
        } ) );
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

    private static RuntimeException clientExceptionWithValidTerminationCause()
    {
        return new ClientException( "¯\\_(ツ)_/¯", serviceUnavailable() );
    }

    private static RuntimeException randomClientException()
    {
        return new ClientException( "Meeh" );
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

    private <T> Mono<T> createMono( T result, Exception... errors )
    {
        AtomicInteger executionCount = new AtomicInteger();
        Iterator<Exception> iterator = Arrays.asList( errors ).iterator();
        return Mono.create( (Consumer<MonoSink<T>>) e -> {
            if ( iterator.hasNext() )
            {
                e.error( iterator.next() );
            }
            else
            {
                e.success( result );
            }
        } ).doOnTerminate( executionCount::getAndIncrement );
    }

    private static Stream<Exception> canBeRetriedErrors()
    {
        return Stream.of(
                transientException(),
                sessionExpired(),
                serviceUnavailable()
        );
    }

    private static Stream<Exception> cannotBeRetriedErrors()
    {
        return Stream.of(
                new IllegalStateException(),
                new TransientException( "Neo.TransientError.Transaction.Terminated", "" ),
                new TransientException( "Neo.TransientError.Transaction.LockClientStopped", "" )
        );
    }
}
