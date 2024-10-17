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
package org.neo4j.driver.internal.reactive;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SuppressWarnings("deprecation")
class InternalRxSessionTest {
    private static Stream<Function<RxSession, RxResult>> allSessionRunMethods() {
        return Stream.of(
                rxSession -> rxSession.run("RETURN 1"),
                rxSession -> rxSession.run("RETURN $x", parameters("x", 1)),
                rxSession -> rxSession.run("RETURN $x", singletonMap("x", 1)),
                rxSession -> rxSession.run(
                        "RETURN $x", new InternalRecord(singletonList("x"), new Value[] {new IntegerValue(1)})),
                rxSession -> rxSession.run(new Query("RETURN $x", parameters("x", 1))),
                rxSession -> rxSession.run(new Query("RETURN $x", parameters("x", 1)), empty()),
                rxSession -> rxSession.run("RETURN $x", singletonMap("x", 1), empty()),
                rxSession -> rxSession.run("RETURN 1", empty()));
    }

    private static Stream<Function<RxSession, Publisher<RxTransaction>>> allBeginTxMethods() {
        return Stream.of(
                RxSession::beginTransaction, rxSession -> rxSession.beginTransaction(TransactionConfig.empty()));
    }

    private static Stream<Function<RxSession, Publisher<String>>> allRunTxMethods() {
        return Stream.of(
                rxSession -> rxSession.readTransaction(tx -> Flux.just("a")),
                rxSession -> rxSession.writeTransaction(tx -> Flux.just("a")),
                rxSession -> rxSession.readTransaction(tx -> Flux.just("a"), empty()),
                rxSession -> rxSession.writeTransaction(tx -> Flux.just("a"), empty()));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldDelegateRun(Function<RxSession, RxResult> runReturnOne) {
        // Given
        var session = mock(NetworkSession.class);
        RxResultCursor cursor = mock(RxResultCursorImpl.class);

        // Run succeeded with a cursor
        when(session.runRx(any(Query.class), any(TransactionConfig.class), any()))
                .thenReturn(completedFuture(cursor));
        var rxSession = new InternalRxSession(session);

        // When
        var result = runReturnOne.apply(rxSession);
        // Execute the run
        var cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify(session).runRx(any(Query.class), any(TransactionConfig.class), any());
        assertThat(Futures.getNow(cursorFuture), equalTo(cursor));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldReleaseConnectionIfFailedToRun(Function<RxSession, RxResult> runReturnOne) {
        // Given
        Throwable error = new RuntimeException("Hi there");
        var session = mock(NetworkSession.class);

        // Run failed with error
        when(session.runRx(any(Query.class), any(TransactionConfig.class), any()))
                .thenReturn(failedFuture(error));
        when(session.releaseConnectionAsync()).thenReturn(Futures.completedWithNull());

        var rxSession = new InternalRxSession(session);

        // When
        var result = runReturnOne.apply(rxSession);
        // Execute the run
        var cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify(session).runRx(any(Query.class), any(TransactionConfig.class), any());
        RuntimeException t = assertThrows(CompletionException.class, () -> Futures.getNow(cursorFuture));
        assertThat(t.getCause(), equalTo(error));
        verify(session).releaseConnectionAsync();
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldDelegateBeginTx(Function<RxSession, Publisher<RxTransaction>> beginTx) {
        // Given
        var session = mock(NetworkSession.class);
        var tx = mock(UnmanagedTransaction.class);
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);

        when(session.beginTransactionAsync(any(TransactionConfig.class), isNull(), eq(apiTelemetryWork)))
                .thenReturn(completedFuture(tx));
        var rxSession = new InternalRxSession(session);

        // When
        var rxTx = beginTx.apply(rxSession);
        StepVerifier.create(Mono.from(rxTx)).expectNextCount(1).verifyComplete();

        // Then
        verify(session).beginTransactionAsync(any(TransactionConfig.class), isNull(), eq(apiTelemetryWork));
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldReleaseConnectionIfFailedToBeginTx(Function<RxSession, Publisher<RxTransaction>> beginTx) {
        // Given
        Throwable error = new RuntimeException("Hi there");
        var session = mock(NetworkSession.class);
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);

        // Run failed with error
        when(session.beginTransactionAsync(any(TransactionConfig.class), isNull(), eq(apiTelemetryWork)))
                .thenReturn(failedFuture(error));
        when(session.releaseConnectionAsync()).thenReturn(Futures.completedWithNull());

        var rxSession = new InternalRxSession(session);

        // When
        var rxTx = beginTx.apply(rxSession);
        var txFuture = Mono.from(rxTx).toFuture();

        // Then
        verify(session).beginTransactionAsync(any(TransactionConfig.class), isNull(), eq(apiTelemetryWork));
        RuntimeException t = assertThrows(CompletionException.class, () -> Futures.getNow(txFuture));
        assertThat(t.getCause(), equalTo(error));
        verify(session).releaseConnectionAsync();
    }

    @ParameterizedTest
    @MethodSource("allRunTxMethods")
    void shouldDelegateRunTx(Function<RxSession, Publisher<String>> runTx) {
        // Given
        var session = mock(NetworkSession.class);
        var tx = mock(UnmanagedTransaction.class);
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.MANAGED_TRANSACTION);
        when(tx.closeAsync(true)).thenReturn(completedWithNull());

        when(session.beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork)))
                .thenReturn(completedFuture(tx));
        when(session.retryLogic()).thenReturn(new FixedRetryLogic(1));
        var rxSession = new InternalRxSession(session);

        // When
        var strings = runTx.apply(rxSession);
        StepVerifier.create(Flux.from(strings)).expectNext("a").verifyComplete();

        // Then
        verify(session)
                .beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork));
        verify(tx).closeAsync(true);
    }

    @Test
    void shouldRetryOnError() {
        // Given
        var retryCount = 2;
        var session = mock(NetworkSession.class);
        var tx = mock(UnmanagedTransaction.class);
        when(tx.closeAsync(false)).thenReturn(completedWithNull());
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.MANAGED_TRANSACTION);

        when(session.beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork)))
                .thenReturn(completedFuture(tx));
        when(session.retryLogic()).thenReturn(new FixedRetryLogic(retryCount));
        var rxSession = new InternalRxSession(session);

        // When
        Publisher<String> strings =
                rxSession.readTransaction(t -> Flux.just("a").then(Mono.error(new RuntimeException("Errored"))));
        StepVerifier.create(Flux.from(strings))
                // we lost the "a"s too as the user only see the last failure
                .expectError(RuntimeException.class)
                .verify();

        // Then
        verify(session, times(retryCount + 1))
                .beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork));
        verify(tx, times(retryCount + 1)).closeAsync(false);
    }

    @Test
    void shouldObtainResultIfRetrySucceed() {
        // Given
        var retryCount = 2;
        var session = mock(NetworkSession.class);
        var tx = mock(UnmanagedTransaction.class);
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.MANAGED_TRANSACTION);
        when(tx.closeAsync(false)).thenReturn(completedWithNull());
        when(tx.closeAsync(true)).thenReturn(completedWithNull());

        when(session.beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork)))
                .thenReturn(completedFuture(tx));
        when(session.retryLogic()).thenReturn(new FixedRetryLogic(retryCount));
        var rxSession = new InternalRxSession(session);

        // When
        var count = new AtomicInteger();
        var strings = rxSession.readTransaction(t -> {
            // we fail for the first few retries, and then success on the last run.
            if (count.getAndIncrement() == retryCount) {
                return Flux.just("a");
            } else {
                return Flux.just("a").then(Mono.error(new RuntimeException("Errored")));
            }
        });
        StepVerifier.create(Flux.from(strings)).expectNext("a").verifyComplete();

        // Then
        verify(session, times(retryCount + 1))
                .beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class), eq(apiTelemetryWork));
        verify(tx, times(retryCount)).closeAsync(false);
        verify(tx).closeAsync(true);
    }

    @Test
    void shouldDelegateBookmark() {
        // Given
        var session = mock(NetworkSession.class);
        var rxSession = new InternalRxSession(session);

        // When
        rxSession.lastBookmark();

        // Then
        verify(session).lastBookmarks();
        verifyNoMoreInteractions(session);
    }

    @Test
    void shouldDelegateBookmarks() {
        // Given
        var session = mock(NetworkSession.class);
        var rxSession = new InternalRxSession(session);

        // When
        rxSession.lastBookmarks();

        // Then
        verify(session).lastBookmarks();
        verifyNoMoreInteractions(session);
    }

    @Test
    void shouldDelegateClose() {
        // Given
        var session = mock(NetworkSession.class);
        when(session.closeAsync()).thenReturn(completedWithNull());
        var rxSession = new InternalRxSession(session);

        // When
        Publisher<Void> mono = rxSession.close();

        // Then
        StepVerifier.create(mono).verifyComplete();
        verify(session).closeAsync();
        verifyNoMoreInteractions(session);
    }
}
