/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.test.StepVerifier;

class ExponentialBackoffRetryLogicTest {
    private final ImmediateSchedulingEventExecutor eventExecutor = new ImmediateSchedulingEventExecutor();

    @Test
    void throwsForIllegalMaxRetryTime() {
        var error = assertThrows(
                IllegalArgumentException.class, () -> newRetryLogic(-100, 1, 1, 1, Clock.systemUTC(), (ignored) -> {}));
        assertThat(error.getMessage(), containsString("Max retry time"));
    }

    @Test
    void throwsForIllegalInitialRetryDelay() {
        var error = assertThrows(
                IllegalArgumentException.class, () -> newRetryLogic(1, -100, 1, 1, Clock.systemUTC(), (ignored) -> {}));
        assertThat(error.getMessage(), containsString("Initial retry delay"));
    }

    @Test
    void throwsForIllegalMultiplier() {
        var error = assertThrows(
                IllegalArgumentException.class, () -> newRetryLogic(1, 1, 1.99, 1, Clock.systemUTC(), (ignored) -> {}));
        assertThat(error.getMessage(), containsString("Multiplier"));
    }

    @Test
    void throwsForIllegalJitterFactor() {
        var error1 = assertThrows(
                IllegalArgumentException.class,
                () -> newRetryLogic(1, 1, 2, -0.42, Clock.systemUTC(), (ignored) -> {}));
        assertThat(error1.getMessage(), containsString("Jitter"));

        var error2 = assertThrows(
                IllegalArgumentException.class, () -> newRetryLogic(1, 1, 2, 1.42, Clock.systemUTC(), (ignored) -> {}));
        assertThat(error2.getMessage(), containsString("Jitter"));
    }

    @Test
    void throwsForIllegalClock() {
        var error =
                assertThrows(IllegalArgumentException.class, () -> newRetryLogic(1, 1, 2, 1, null, (ignored) -> {}));
        assertThat(error.getMessage(), containsString("Clock"));
    }

    @Test
    void shouldInitialiseWithExpectedDefaultValues() {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = new ExponentialBackoffRetryLogic(MAX_VALUE, eventExecutor, clock, DEV_NULL_LOGGING, sleepTask);

        assertEquals(SECONDS.toMillis(1), logic.initialRetryDelayMs);
        assertEquals(2.0, logic.multiplier);
        assertEquals(0.2, logic.jitterFactor);
    }

    @Test
    void nextDelayCalculatedAccordingToMultiplier() throws Exception {
        var retries = 27;
        var initialDelay = 1;
        var multiplier = 3;
        var noJitter = 0;
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(MAX_VALUE, initialDelay, multiplier, noJitter, clock, sleepTask);

        retry(logic, retries);

        assertEquals(delaysWithoutJitter(initialDelay, multiplier, retries), sleepValues(sleepTask, retries));
    }

    @Test
    void nextDelayCalculatedAccordingToMultiplierAsync() {
        var result = "The Result";
        var retries = 14;
        var initialDelay = 1;
        var multiplier = 2;
        var noJitter = 0;

        var retryLogic =
                newRetryLogic(MAX_VALUE, initialDelay, multiplier, noJitter, Clock.systemUTC(), (ignored) -> {});

        var future = retryAsync(retryLogic, retries, result);

        assertEquals(result, await(future));
        assertEquals(delaysWithoutJitter(initialDelay, multiplier, retries), eventExecutor.scheduleDelays());
    }

    @Test
    void nextDelayCalculatedAccordingToMultiplierRx() {
        var result = "The Result";
        var retries = 14;
        var initialDelay = 1;
        var multiplier = 2;
        var noJitter = 0;

        var retryLogic =
                newRetryLogic(MAX_VALUE, initialDelay, multiplier, noJitter, Clock.systemUTC(), (ignored) -> {});

        var single = Flux.from(retryRx(retryLogic, retries, result)).single();

        assertEquals(result, await(single));
        assertEquals(delaysWithoutJitter(initialDelay, multiplier, retries), eventExecutor.scheduleDelays());
    }

    @Test
    void nextDelayCalculatedAccordingToJitter() throws Exception {
        var retries = 32;
        var jitterFactor = 0.2;
        var initialDelay = 1;
        var multiplier = 2;
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(MAX_VALUE, initialDelay, multiplier, jitterFactor, clock, sleepTask);

        retry(logic, retries);

        var sleepValues = sleepValues(sleepTask, retries);
        var delaysWithoutJitter = delaysWithoutJitter(initialDelay, multiplier, retries);

        assertDelaysApproximatelyEqual(delaysWithoutJitter, sleepValues, jitterFactor);
    }

    @Test
    void nextDelayCalculatedAccordingToJitterAsync() {
        var result = "The Result";
        var retries = 24;
        var jitterFactor = 0.2;
        var initialDelay = 1;
        var multiplier = 2;

        var retryLogic =
                newRetryLogic(MAX_VALUE, initialDelay, multiplier, jitterFactor, mock(Clock.class), (ignored) -> {});

        var future = retryAsync(retryLogic, retries, result);
        assertEquals(result, await(future));

        var scheduleDelays = eventExecutor.scheduleDelays();
        var delaysWithoutJitter = delaysWithoutJitter(initialDelay, multiplier, retries);

        assertDelaysApproximatelyEqual(delaysWithoutJitter, scheduleDelays, jitterFactor);
    }

    @Test
    void nextDelayCalculatedAccordingToJitterRx() {
        var result = "The Result";
        var retries = 24;
        var jitterFactor = 0.2;
        var initialDelay = 1;
        var multiplier = 2;

        var retryLogic =
                newRetryLogic(MAX_VALUE, initialDelay, multiplier, jitterFactor, mock(Clock.class), (ignored) -> {});

        var single = Flux.from(retryRx(retryLogic, retries, result)).single();
        assertEquals(result, await(single));

        var scheduleDelays = eventExecutor.scheduleDelays();
        var delaysWithoutJitter = delaysWithoutJitter(initialDelay, multiplier, retries);

        assertDelaysApproximatelyEqual(delaysWithoutJitter, scheduleDelays, jitterFactor);
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceeded() throws Exception {
        var retryStart = Clock.systemUTC().millis();
        var initialDelay = 100;
        var multiplier = 2;
        long maxRetryTimeMs = 45;
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        when(clock.millis())
                .thenReturn(retryStart)
                .thenReturn(retryStart + maxRetryTimeMs - 5)
                .thenReturn(retryStart + maxRetryTimeMs + 7);

        var logic = newRetryLogic(maxRetryTimeMs, initialDelay, multiplier, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenThrow(error);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error, e);

        verify(sleepTask).sleep(initialDelay);
        verify(sleepTask).sleep(initialDelay * multiplier);
        verify(workMock, times(3)).get();
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceededAsync() {
        var retryStart = Clock.systemUTC().millis();
        var initialDelay = 100;
        var multiplier = 2;
        long maxRetryTimeMs = 45;
        var clock = mock(Clock.class);
        when(clock.millis())
                .thenReturn(retryStart)
                .thenReturn(retryStart + maxRetryTimeMs - 5)
                .thenReturn(retryStart + maxRetryTimeMs + 7);

        var retryLogic = newRetryLogic(maxRetryTimeMs, initialDelay, multiplier, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenReturn(failedFuture(error));

        var future = retryLogic.retryAsync(workMock);

        var e = assertThrows(Exception.class, () -> await(future));
        assertEquals(error, e);

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(2, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());

        verify(workMock, times(3)).get();
    }

    @Test
    void doesNotRetryWhenMaxRetryTimeExceededRx() {
        var retryStart = Clock.systemUTC().millis();
        var initialDelay = 100;
        var multiplier = 2;
        long maxRetryTimeMs = 45;
        var clock = mock(Clock.class);
        when(clock.millis())
                .thenReturn(retryStart)
                .thenReturn(retryStart + maxRetryTimeMs - 5)
                .thenReturn(retryStart + maxRetryTimeMs + 7);

        var retryLogic = newRetryLogic(maxRetryTimeMs, initialDelay, multiplier, 0, clock, (ignored) -> {});

        var error = sessionExpired();
        var executionCount = new AtomicInteger();
        var publisher = retryLogic.retryRx(Mono.error(error).doOnTerminate(executionCount::getAndIncrement));

        var e = assertThrows(Exception.class, () -> await(publisher));
        assertEquals(error, e);

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(2, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());

        assertThat(executionCount.get(), equalTo(3));
    }

    @Test
    void sleepsOnServiceUnavailableException() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 42, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = serviceUnavailable();
        when(workMock.get()).thenThrow(error).thenReturn(null);

        assertNull(logic.retry(workMock));

        verify(workMock, times(2)).get();
        verify(sleepTask).sleep(42);
    }

    @Test
    void schedulesRetryOnServiceUnavailableExceptionAsync() {
        var result = "The Result";
        var clock = mock(Clock.class);

        var retryLogic = newRetryLogic(1, 42, 2, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = serviceUnavailable();
        when(workMock.get()).thenReturn(failedFuture(error)).thenReturn(completedFuture(result));

        assertEquals(result, await(retryLogic.retryAsync(workMock)));

        verify(workMock, times(2)).get();
        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(1, scheduleDelays.size());
        assertEquals(42, scheduleDelays.get(0).intValue());
    }

    @Test
    void sleepsOnSessionExpiredException() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 4242, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenThrow(error).thenReturn(null);

        assertNull(logic.retry(workMock));

        verify(workMock, times(2)).get();
        verify(sleepTask).sleep(4242);
    }

    @Test
    void schedulesRetryOnSessionExpiredExceptionAsync() {
        var result = "The Result";
        var clock = mock(Clock.class);

        var retryLogic = newRetryLogic(1, 4242, 2, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenReturn(failedFuture(error)).thenReturn(completedFuture(result));

        assertEquals(result, await(retryLogic.retryAsync(workMock)));

        verify(workMock, times(2)).get();
        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(1, scheduleDelays.size());
        assertEquals(4242, scheduleDelays.get(0).intValue());
    }

    @Test
    void sleepsOnTransientException() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 23, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = transientException();
        when(workMock.get()).thenThrow(error).thenReturn(null);

        assertNull(logic.retry(workMock));

        verify(workMock, times(2)).get();
        verify(sleepTask).sleep(23);
    }

    @Test
    void schedulesRetryOnTransientExceptionAsync() {
        var result = "The Result";
        var clock = mock(Clock.class);

        var retryLogic = newRetryLogic(1, 23, 2, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = transientException();
        when(workMock.get()).thenReturn(failedFuture(error)).thenReturn(completedFuture(result));

        assertEquals(result, await(retryLogic.retryAsync(workMock)));

        verify(workMock, times(2)).get();
        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(1, scheduleDelays.size());
        assertEquals(23, scheduleDelays.get(0).intValue());
    }

    @Test
    void throwsWhenUnknownError() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 1, 2, 1, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = new IllegalStateException();
        when(workMock.get()).thenThrow(error);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error, e);

        verify(workMock).get();
        verify(sleepTask, never()).sleep(anyLong());
    }

    @Test
    void doesNotRetryOnUnknownErrorAsync() {
        var clock = mock(Clock.class);
        var retryLogic = newRetryLogic(1, 1, 2, 1, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = new IllegalStateException();
        when(workMock.get()).thenReturn(failedFuture(error));

        var e = assertThrows(Exception.class, () -> await(retryLogic.retryAsync(workMock)));
        assertEquals(error, e);

        verify(workMock).get();
        assertEquals(0, eventExecutor.scheduleDelays().size());
    }

    @Test
    void throwsWhenTransactionTerminatedError() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 13, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = new ClientException("Neo.ClientError.Transaction.Terminated", "");
        when(workMock.get()).thenThrow(error).thenReturn(null);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error, e);

        verify(workMock).get();
        verify(sleepTask, never()).sleep(13);
    }

    @Test
    void doesNotRetryOnTransactionTerminatedErrorAsync() {
        var clock = mock(Clock.class);
        var retryLogic = newRetryLogic(1, 13, 2, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = new ClientException("Neo.ClientError.Transaction.Terminated", "");
        when(workMock.get()).thenReturn(failedFuture(error));

        var e = assertThrows(Exception.class, () -> await(retryLogic.retryAsync(workMock)));
        assertEquals(error, e);

        verify(workMock).get();
        assertEquals(0, eventExecutor.scheduleDelays().size());
    }

    @Test
    void throwsWhenTransactionLockClientStoppedError() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(1, 13, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = new ClientException("Neo.ClientError.Transaction.LockClientStopped", "");
        when(workMock.get()).thenThrow(error).thenReturn(null);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error, e);

        verify(workMock).get();
        verify(sleepTask, never()).sleep(13);
    }

    @Test
    void doesNotRetryOnTransactionLockClientStoppedErrorAsync() {
        var clock = mock(Clock.class);
        var retryLogic = newRetryLogic(1, 15, 2, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = new ClientException("Neo.ClientError.Transaction.LockClientStopped", "");
        when(workMock.get()).thenReturn(failedFuture(error));

        var e = assertThrows(Exception.class, () -> await(retryLogic.retryAsync(workMock)));
        assertEquals(error, e);

        verify(workMock).get();
        assertEquals(0, eventExecutor.scheduleDelays().size());
    }

    @ParameterizedTest
    @MethodSource("canBeRetriedErrors")
    void schedulesRetryOnErrorRx(Exception error) {
        var result = "The Result";
        var clock = mock(Clock.class);
        var retryLogic = newRetryLogic(1, 4242, 2, 0, clock, (ignored) -> {});

        Publisher<String> publisher = createMono(result, error);
        var single = Flux.from(retryLogic.retryRx(publisher)).single();

        assertEquals(result, await(single));

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(1, scheduleDelays.size());
        assertEquals(4242, scheduleDelays.get(0).intValue());
    }

    @ParameterizedTest
    @MethodSource("cannotBeRetriedErrors")
    void scheduleNoRetryOnErrorRx(Exception error) {
        var clock = mock(Clock.class);
        var retryLogic = newRetryLogic(1, 10, 2, 1, clock, (ignored) -> {});

        var single = Flux.from(retryLogic.retryRx(Mono.error(error))).single();

        var e = assertThrows(Exception.class, () -> await(single));
        assertEquals(error, e);

        assertEquals(0, eventExecutor.scheduleDelays().size());
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void throwsWhenSleepInterrupted() throws Exception {
        var clock = mock(Clock.class);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        doThrow(new InterruptedException()).when(sleepTask).sleep(1);
        var logic = newRetryLogic(1, 1, 2, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        when(workMock.get()).thenThrow(serviceUnavailable());

        try {
            var e = assertThrows(IllegalStateException.class, () -> logic.retry(workMock));
            assertThat(e.getCause(), instanceOf(InterruptedException.class));
        } finally {
            // Clear the interruption flag so all subsequent tests do not see this thread as interrupted
            Thread.interrupted();
        }
    }

    @Test
    void collectsSuppressedErrors() throws Exception {
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(15L).thenReturn(25L);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error1 = sessionExpired();
        var error2 = sessionExpired();
        var error3 = serviceUnavailable();
        var error4 = transientException();
        when(workMock.get()).thenThrow(error1, error2, error3, error4).thenReturn(null);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error4, e);
        var suppressed = e.getSuppressed();
        assertEquals(3, suppressed.length);
        assertEquals(error1, suppressed[0]);
        assertEquals(error2, suppressed[1]);
        assertEquals(error3, suppressed[2]);

        verify(workMock, times(4)).get();

        verify(sleepTask, times(3)).sleep(anyLong());
        verify(sleepTask).sleep(initialDelay);
        verify(sleepTask).sleep(initialDelay * multiplier);
        verify(sleepTask).sleep(initialDelay * multiplier * multiplier);
    }

    @Test
    void collectsSuppressedErrorsAsync() {
        var result = "The Result";
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(15L).thenReturn(25L);

        var retryLogic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error1 = sessionExpired();
        var error2 = sessionExpired();
        var error3 = serviceUnavailable();
        var error4 = transientException();

        when(workMock.get())
                .thenReturn(failedFuture(error1))
                .thenReturn(failedFuture(error2))
                .thenReturn(failedFuture(error3))
                .thenReturn(failedFuture(error4))
                .thenReturn(completedFuture(result));

        var e = assertThrows(Exception.class, () -> await(retryLogic.retryAsync(workMock)));
        assertEquals(error4, e);
        var suppressed = e.getSuppressed();
        assertEquals(3, suppressed.length);
        assertEquals(error1, suppressed[0]);
        assertEquals(error2, suppressed[1]);
        assertEquals(error3, suppressed[2]);

        verify(workMock, times(4)).get();

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(3, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());
        assertEquals(
                initialDelay * multiplier * multiplier, scheduleDelays.get(2).intValue());
    }

    @Test
    void collectsSuppressedErrorsRx() {
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(15L).thenReturn(25L);
        var logic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, (ignored) -> {});

        var error1 = sessionExpired();
        var error2 = sessionExpired();
        var error3 = serviceUnavailable();
        var error4 = transientException();
        var mono = createMono("A result", error1, error2, error3, error4);

        StepVerifier.create(logic.retryRx(mono))
                .expectErrorSatisfies(e -> {
                    assertEquals(error4, e);
                    var suppressed = e.getSuppressed();
                    assertEquals(3, suppressed.length);
                    assertEquals(error1, suppressed[0]);
                    assertEquals(error2, suppressed[1]);
                    assertEquals(error3, suppressed[2]);
                })
                .verify();

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(3, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());
        assertEquals(
                initialDelay * multiplier * multiplier, scheduleDelays.get(2).intValue());
    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrown() throws Exception {
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(25L);
        var sleepTask = mock(ExponentialBackoffRetryLogic.SleepTask.class);
        var logic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, sleepTask);

        Supplier<Void> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenThrow(error);

        var e = assertThrows(Exception.class, () -> logic.retry(workMock));
        assertEquals(error, e);
        assertEquals(0, e.getSuppressed().length);

        verify(workMock, times(3)).get();

        verify(sleepTask, times(2)).sleep(anyLong());
        verify(sleepTask).sleep(initialDelay);
        verify(sleepTask).sleep(initialDelay * multiplier);
    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrownAsync() {
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(25L);

        var retryLogic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, (ignored) -> {});

        Supplier<CompletionStage<Object>> workMock = newWorkMock();
        var error = sessionExpired();
        when(workMock.get()).thenReturn(failedFuture(error));

        var e = assertThrows(Exception.class, () -> await(retryLogic.retryAsync(workMock)));
        assertEquals(error, e);
        assertEquals(0, e.getSuppressed().length);

        verify(workMock, times(3)).get();

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(2, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());
    }

    @Test
    void doesNotCollectSuppressedErrorsWhenSameErrorIsThrownRx() {
        long maxRetryTime = 20;
        var initialDelay = 15;
        var multiplier = 2;
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L).thenReturn(10L).thenReturn(25L);

        var retryLogic = newRetryLogic(maxRetryTime, initialDelay, multiplier, 0, clock, (ignored) -> {});

        var error = sessionExpired();
        StepVerifier.create(retryLogic.retryRx(Mono.error(error)))
                .expectErrorSatisfies(e -> assertEquals(error, e))
                .verify();

        var scheduleDelays = eventExecutor.scheduleDelays();
        assertEquals(2, scheduleDelays.size());
        assertEquals(initialDelay, scheduleDelays.get(0).intValue());
        assertEquals(initialDelay * multiplier, scheduleDelays.get(1).intValue());
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCause() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = logic.retry(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw clientExceptionWithValidTerminationCause();
            }
            return "Done";
        });

        assertEquals("Done", result);
    }

    @Test
    void doesRetryOnAuthorizationExpiredException() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = logic.retry(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw authorizationExpiredException();
            }
            return "Done";
        });

        assertEquals("Done", result);
    }

    @Test
    void doesRetryOnConnectionReadTimeoutException() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = logic.retry(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw ConnectionReadTimeoutException.INSTANCE;
            }
            return "Done";
        });

        assertEquals("Done", result);
    }

    @Test
    void doesNotRetryOnRandomClientException() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var exception = Assertions.assertThrows(
                ClientException.class,
                () -> logic.retry(() -> {
                    if (exceptionThrown.compareAndSet(false, true)) {
                        throw randomClientException();
                    }
                    return "Done";
                }));

        assertEquals("Meeh", exception.getMessage());
    }

    @Test
    void eachRetryIsLogged() {
        var retries = 9;
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic = new ExponentialBackoffRetryLogic(
                RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging, (ignored) -> {});

        retry(logic, retries);

        verify(logger, times(retries))
                .warn(startsWith("Transaction failed and will be retried"), any(ServiceUnavailableException.class));
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCauseAsync() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = await(logic.retryAsync(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw clientExceptionWithValidTerminationCause();
            }
            return CompletableFuture.completedFuture("Done");
        }));

        assertEquals("Done", result);
    }

    @Test
    void doesRetryOnAuthorizationExpiredExceptionAsync() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = await(logic.retryAsync(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw authorizationExpiredException();
            }
            return CompletableFuture.completedFuture("Done");
        }));

        assertEquals("Done", result);
    }

    @Test
    void doesNotRetryOnRandomClientExceptionAsync() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var exception = Assertions.assertThrows(
                ClientException.class,
                () -> await(logic.retryAsync(() -> {
                    if (exceptionThrown.compareAndSet(false, true)) {
                        throw randomClientException();
                    }
                    return CompletableFuture.completedFuture("Done");
                })));

        assertEquals("Meeh", exception.getMessage());
    }

    @Test
    void eachRetryIsLoggedAsync() {
        var result = "The Result";
        var retries = 9;
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        assertEquals(result, await(retryAsync(logic, retries, result)));

        verify(logger, times(retries))
                .warn(
                        startsWith("Async transaction failed and is scheduled to retry"),
                        any(ServiceUnavailableException.class));
    }

    @Test
    void doesRetryOnClientExceptionWithRetryableCauseRx() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = await(Mono.from(logic.retryRx(Mono.fromSupplier(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw clientExceptionWithValidTerminationCause();
            }
            return "Done";
        }))));

        assertEquals("Done", result);
    }

    @Test
    void doesRetryOnAuthorizationExpiredExceptionRx() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = await(Mono.from(logic.retryRx(Mono.fromSupplier(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw authorizationExpiredException();
            }
            return "Done";
        }))));

        assertEquals("Done", result);
    }

    @Test
    void doesRetryOnAsyncResourceCleanupRuntimeExceptionRx() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var result = await(Mono.from(logic.retryRx(Mono.fromSupplier(() -> {
            if (exceptionThrown.compareAndSet(false, true)) {
                throw new RuntimeException("Async resource cleanup failed after", authorizationExpiredException());
            }
            return "Done";
        }))));

        assertEquals("Done", result);
    }

    @Test
    void doesNotRetryOnRandomClientExceptionRx() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        var exceptionThrown = new AtomicBoolean(false);
        var exception = Assertions.assertThrows(
                ClientException.class,
                () -> await(Mono.from(logic.retryRx(Mono.fromSupplier(() -> {
                    if (exceptionThrown.compareAndSet(false, true)) {
                        throw randomClientException();
                    }
                    return "Done";
                })))));

        assertEquals("Meeh", exception.getMessage());
    }

    @Test
    void eachRetryIsLoggedRx() {
        var result = "The Result";
        var retries = 9;
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);

        var logic =
                new ExponentialBackoffRetryLogic(RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, clock, logging);

        assertEquals(result, await(Flux.from(retryRx(logic, retries, result)).single()));

        verify(logger, times(retries))
                .warn(
                        startsWith("Reactive transaction failed and is scheduled to retry"),
                        any(ServiceUnavailableException.class));
    }

    @Test
    void nothingIsLoggedOnFatalFailure() {
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);
        var logic = new ExponentialBackoffRetryLogic(
                RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, mock(Clock.class), logging);

        var error = assertThrows(
                RuntimeException.class,
                () -> logic.retry(() -> {
                    throw new RuntimeException("Fatal blocking");
                }));
        assertEquals("Fatal blocking", error.getMessage());
        verifyNoInteractions(logger);
    }

    @Test
    void nothingIsLoggedOnFatalFailureAsync() {
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);
        var logic = new ExponentialBackoffRetryLogic(
                RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, mock(Clock.class), logging);

        var error = assertThrows(
                RuntimeException.class,
                () -> await(logic.retryAsync(() -> failedFuture(new RuntimeException("Fatal async")))));

        assertEquals("Fatal async", error.getMessage());
        verifyNoInteractions(logger);
    }

    @Test
    void nothingIsLoggedOnFatalFailureRx() {
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(anyString())).thenReturn(logger);
        var logic = new ExponentialBackoffRetryLogic(
                RetrySettings.DEFAULT.maxRetryTimeMs(), eventExecutor, mock(Clock.class), logging);

        var retryRx = logic.retryRx(Mono.error(new RuntimeException("Fatal rx")));
        var error = assertThrows(RuntimeException.class, () -> await(retryRx));

        assertEquals("Fatal rx", error.getMessage());
        verifyNoInteractions(logger);
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailure() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic(settings.maxRetryTimeMs(), eventExecutor, clock, logging);

        var error = assertThrows(
                ServiceUnavailableException.class,
                () -> logic.retry(new Supplier<Long>() {
                    boolean invoked;

                    @Override
                    public Long get() {
                        // work that always fails and moves clock forward after the first failure
                        if (invoked) {
                            // move clock forward to stop retries
                            when(clock.millis()).thenReturn(settings.maxRetryTimeMs() + 42);
                        } else {
                            invoked = true;
                        }
                        throw new ServiceUnavailableException("Error");
                    }
                }));
        assertEquals("Error", error.getMessage());
        verify(logger)
                .warn(startsWith("Transaction failed and will be retried"), any(ServiceUnavailableException.class));
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailureAsync() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic(settings.maxRetryTimeMs(), eventExecutor, clock, logging);

        var error = assertThrows(
                SessionExpiredException.class,
                () -> await(logic.retryAsync(new Supplier<CompletionStage<Void>>() {
                    volatile boolean invoked;

                    @Override
                    public CompletionStage<Void> get() {
                        // work that always returns failed future and moves clock forward after the first failure
                        if (invoked) {
                            // move clock forward to stop retries
                            when(clock.millis()).thenReturn(settings.maxRetryTimeMs() + 42);
                        } else {
                            invoked = true;
                        }
                        return failedFuture(new SessionExpiredException("Session no longer valid"));
                    }
                })));
        assertEquals("Session no longer valid", error.getMessage());
        verify(logger)
                .warn(
                        startsWith("Async transaction failed and is scheduled to retry"),
                        any(SessionExpiredException.class));
    }

    @Test
    void correctNumberOfRetiesAreLoggedOnFailureRx() {
        var clock = mock(Clock.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        var settings = RetrySettings.DEFAULT;
        RetryLogic logic = new ExponentialBackoffRetryLogic(settings.maxRetryTimeMs(), eventExecutor, clock, logging);

        var invoked = new AtomicBoolean(false);
        var error = assertThrows(
                SessionExpiredException.class,
                () -> await(logic.retryRx(Mono.create(e -> {
                    if (invoked.get()) {
                        // move clock forward to stop retries
                        when(clock.millis()).thenReturn(settings.maxRetryTimeMs() + 42);
                    } else {
                        invoked.set(true);
                    }
                    e.error(new SessionExpiredException("Session no longer valid"));
                }))));
        assertEquals("Session no longer valid", error.getMessage());
        verify(logger)
                .warn(
                        startsWith("Reactive transaction failed and is scheduled to retry"),
                        any(SessionExpiredException.class));
    }

    @Test
    void shouldRetryWithBackOffRx() {
        Exception exception = new TransientException("Unknown", "Retry this error.");
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L, 100L, 200L, 400L, 800L);
        var retryLogic = new ExponentialBackoffRetryLogic(
                500, 100, 2, 0, eventExecutor, clock, DEV_NULL_LOGGING, (ignored) -> {});

        var source = Flux.concat(Flux.range(0, 2), Flux.error(exception));
        var retriedSource = Flux.from(retryLogic.retryRx(source));
        StepVerifier.create(retriedSource)
                .expectNext(0, 1) // first run
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1) // 4 retry attempts
                .verifyErrorSatisfies(e -> assertThat(e, equalTo(exception)));

        var delays = eventExecutor.scheduleDelays();
        assertThat(delays.size(), equalTo(4));
        assertThat(delays, contains(100L, 200L, 400L, 800L));
    }

    @Test
    void shouldRetryWithRandomBackOffRx() {
        Exception exception = new TransientException("Unknown", "Retry this error.");
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L, 100L, 200L, 400L, 800L);
        var retryLogic = new ExponentialBackoffRetryLogic(
                500, 100, 2, 0.1, eventExecutor, clock, DEV_NULL_LOGGING, (ignored) -> {});

        var source = Flux.concat(Flux.range(0, 2), Flux.error(exception));
        var retriedSource = Flux.from(retryLogic.retryRx(source));
        StepVerifier.create(retriedSource)
                .expectNext(0, 1) // first run
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1) // 4 retry attempts
                .verifyErrorSatisfies(e -> assertThat(e, equalTo(exception)));

        var delays = eventExecutor.scheduleDelays();
        assertThat(delays.size(), equalTo(4));
        assertThat(delays.get(0), allOf(greaterThanOrEqualTo(90L), lessThanOrEqualTo(110L)));
        assertThat(delays.get(1), allOf(greaterThanOrEqualTo(180L), lessThanOrEqualTo(220L)));
        assertThat(delays.get(2), allOf(greaterThanOrEqualTo(260L), lessThanOrEqualTo(440L)));
        assertThat(delays.get(3), allOf(greaterThanOrEqualTo(720L), lessThanOrEqualTo(880L)));
    }

    private static void retry(ExponentialBackoffRetryLogic retryLogic, final int times) {
        retryLogic.retry(new Supplier<Void>() {
            int invoked;

            @Override
            public Void get() {
                if (invoked < times) {
                    invoked++;
                    throw serviceUnavailable();
                }
                return null;
            }
        });
    }

    private CompletionStage<Object> retryAsync(ExponentialBackoffRetryLogic retryLogic, int times, Object result) {
        return retryLogic.retryAsync(new Supplier<>() {
            int invoked;

            @Override
            public CompletionStage<Object> get() {
                if (invoked < times) {
                    invoked++;
                    return failedFuture(serviceUnavailable());
                }
                return completedFuture(result);
            }
        });
    }

    private Publisher<Object> retryRx(ExponentialBackoffRetryLogic retryLogic, int times, Object result) {
        var invoked = new AtomicInteger();
        return retryLogic.retryRx(Mono.create(e -> {
            if (invoked.get() < times) {
                invoked.getAndIncrement();
                e.error(serviceUnavailable());
            } else {
                e.success(result);
            }
        }));
    }

    private static List<Long> delaysWithoutJitter(long initialDelay, double multiplier, int count) {
        List<Long> values = new ArrayList<>();
        var delay = initialDelay;
        do {
            values.add(delay);
            delay *= (long) multiplier;
        } while (--count > 0);
        return values;
    }

    private static List<Long> sleepValues(ExponentialBackoffRetryLogic.SleepTask sleepTask, int expectedCount)
            throws InterruptedException {
        var captor = ArgumentCaptor.forClass(long.class);
        verify(sleepTask, times(expectedCount)).sleep(captor.capture());
        return captor.getAllValues();
    }

    private ExponentialBackoffRetryLogic newRetryLogic(
            long maxRetryTimeMs,
            long initialRetryDelayMs,
            double multiplier,
            double jitterFactor,
            Clock clock,
            ExponentialBackoffRetryLogic.SleepTask sleepTask) {
        return new ExponentialBackoffRetryLogic(
                maxRetryTimeMs,
                initialRetryDelayMs,
                multiplier,
                jitterFactor,
                eventExecutor,
                clock,
                DEV_NULL_LOGGING,
                sleepTask);
    }

    private static ServiceUnavailableException serviceUnavailable() {
        return new ServiceUnavailableException("");
    }

    private static RuntimeException clientExceptionWithValidTerminationCause() {
        return new TransactionTerminatedException("¯\\_(ツ)_/¯", serviceUnavailable());
    }

    private static RuntimeException randomClientException() {
        return new ClientException("Meeh");
    }

    private static SessionExpiredException sessionExpired() {
        return new SessionExpiredException("");
    }

    private static TransientException transientException() {
        return new TransientException("", "");
    }

    private static AuthorizationExpiredException authorizationExpiredException() {
        return new AuthorizationExpiredException("", "");
    }

    @SuppressWarnings("unchecked")
    private static <T> Supplier<T> newWorkMock() {
        return mock(Supplier.class);
    }

    private static void assertDelaysApproximatelyEqual(
            List<Long> expectedDelays, List<Long> actualDelays, double delta) {
        assertEquals(expectedDelays.size(), actualDelays.size());

        for (var i = 0; i < actualDelays.size(); i++) {
            var actualValue = actualDelays.get(i).doubleValue();
            long expectedValue = expectedDelays.get(i);

            assertThat(actualValue, closeTo(expectedValue, expectedValue * delta));
        }
    }

    private <T> Mono<T> createMono(T result, Exception... errors) {
        var executionCount = new AtomicInteger();
        var iterator = Arrays.asList(errors).iterator();
        return Mono.create((Consumer<MonoSink<T>>) e -> {
                    if (iterator.hasNext()) {
                        e.error(iterator.next());
                    } else {
                        e.success(result);
                    }
                })
                .doOnTerminate(executionCount::getAndIncrement);
    }

    private static Stream<Exception> canBeRetriedErrors() {
        return Stream.of(transientException(), sessionExpired(), serviceUnavailable());
    }

    private static Stream<Exception> cannotBeRetriedErrors() {
        return Stream.of(
                new IllegalStateException(),
                new ClientException("Neo.ClientError.Transaction.Terminated", ""),
                new ClientException("Neo.ClientError.Transaction.LockClientStopped", ""));
    }
}
