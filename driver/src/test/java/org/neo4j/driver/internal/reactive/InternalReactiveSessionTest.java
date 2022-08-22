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
package org.neo4j.driver.internal.reactive;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hamcrest.MatcherAssert;
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
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.ReactiveTransaction;
import org.neo4j.driver.reactive.ReactiveTransactionCallback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class InternalReactiveSessionTest {
    private static Stream<Function<ReactiveSession, Publisher<ReactiveResult>>> allSessionRunMethods() {
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

    private static Stream<Function<ReactiveSession, Publisher<ReactiveTransaction>>> allBeginTxMethods() {
        return Stream.of(
                ReactiveSession::beginTransaction, rxSession -> rxSession.beginTransaction(TransactionConfig.empty()));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldDelegateRun(Function<ReactiveSession, Publisher<ReactiveResult>> runReturnOne) {
        // Given
        NetworkSession session = mock(NetworkSession.class);
        RxResultCursor cursor = mock(RxResultCursorImpl.class);

        // Run succeeded with a cursor
        when(session.runRx(any(Query.class), any(TransactionConfig.class))).thenReturn(completedFuture(cursor));
        InternalReactiveSession rxSession = new InternalReactiveSession(session);

        // When
        var result = flowPublisherToFlux(runReturnOne.apply(rxSession));

        // Then
        verify(session).runRx(any(Query.class), any(TransactionConfig.class));
        StepVerifier.create(result).expectNextCount(1).verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldReleaseConnectionIfFailedToRun(Function<ReactiveSession, Publisher<ReactiveResult>> runReturnOne) {
        // Given
        Throwable error = new RuntimeException("Hi there");
        NetworkSession session = mock(NetworkSession.class);

        // Run failed with error
        when(session.runRx(any(Query.class), any(TransactionConfig.class))).thenReturn(Futures.failedFuture(error));
        when(session.releaseConnectionAsync()).thenReturn(Futures.completedWithNull());

        InternalReactiveSession rxSession = new InternalReactiveSession(session);

        // When
        var result = flowPublisherToFlux(runReturnOne.apply(rxSession));

        // Then
        StepVerifier.create(result).expectErrorMatches(t -> error == t).verify();
        verify(session).runRx(any(Query.class), any(TransactionConfig.class));
        verify(session).releaseConnectionAsync();
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldDelegateBeginTx(Function<ReactiveSession, Publisher<ReactiveTransaction>> beginTx) {
        // Given
        NetworkSession session = mock(NetworkSession.class);
        UnmanagedTransaction tx = mock(UnmanagedTransaction.class);

        when(session.beginTransactionAsync(any(TransactionConfig.class))).thenReturn(completedFuture(tx));
        InternalReactiveSession rxSession = new InternalReactiveSession(session);

        // When
        var rxTx = flowPublisherToFlux(beginTx.apply(rxSession));
        StepVerifier.create(Mono.from(rxTx)).expectNextCount(1).verifyComplete();

        // Then
        verify(session).beginTransactionAsync(any(TransactionConfig.class));
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldReleaseConnectionIfFailedToBeginTx(Function<ReactiveSession, Publisher<ReactiveTransaction>> beginTx) {
        // Given
        Throwable error = new RuntimeException("Hi there");
        NetworkSession session = mock(NetworkSession.class);

        // Run failed with error
        when(session.beginTransactionAsync(any(TransactionConfig.class))).thenReturn(Futures.failedFuture(error));
        when(session.releaseConnectionAsync()).thenReturn(Futures.completedWithNull());

        InternalReactiveSession rxSession = new InternalReactiveSession(session);

        // When
        var rxTx = flowPublisherToFlux(beginTx.apply(rxSession));
        CompletableFuture<ReactiveTransaction> txFuture = Mono.from(rxTx).toFuture();

        // Then
        verify(session).beginTransactionAsync(any(TransactionConfig.class));
        RuntimeException t = assertThrows(CompletionException.class, () -> Futures.getNow(txFuture));
        MatcherAssert.assertThat(t.getCause(), equalTo(error));
        verify(session).releaseConnectionAsync();
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldRetryOnError() {
        // Given
        int retryCount = 2;
        NetworkSession session = mock(NetworkSession.class);
        UnmanagedTransaction tx = mock(UnmanagedTransaction.class);
        when(tx.closeAsync(false)).thenReturn(completedWithNull());

        when(session.beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class)))
                .thenReturn(completedFuture(tx));
        when(session.retryLogic()).thenReturn(new FixedRetryLogic(retryCount));
        InternalRxSession rxSession = new InternalRxSession(session);

        // When
        var strings = rxSession.<String>readTransaction(
                t -> Flux.just("a").then(Mono.error(new RuntimeException("Errored"))));
        StepVerifier.create(Flux.from(strings))
                // we lost the "a"s too as the user only see the last failure
                .expectError(RuntimeException.class)
                .verify();

        // Then
        verify(session, times(retryCount + 1))
                .beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class));
        verify(tx, times(retryCount + 1)).closeAsync(false);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldObtainResultIfRetrySucceed() {
        // Given
        int retryCount = 2;
        NetworkSession session = mock(NetworkSession.class);
        UnmanagedTransaction tx = mock(UnmanagedTransaction.class);
        when(tx.closeAsync(false)).thenReturn(completedWithNull());
        when(tx.closeAsync(true)).thenReturn(completedWithNull());

        when(session.beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class)))
                .thenReturn(completedFuture(tx));
        when(session.retryLogic()).thenReturn(new FixedRetryLogic(retryCount));
        InternalRxSession rxSession = new InternalRxSession(session);

        // When
        AtomicInteger count = new AtomicInteger();
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
                .beginTransactionAsync(any(AccessMode.class), any(TransactionConfig.class));
        verify(tx, times(retryCount)).closeAsync(false);
        verify(tx).closeAsync(true);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDelegateBookmark() {
        // Given
        NetworkSession session = mock(NetworkSession.class);
        InternalRxSession rxSession = new InternalRxSession(session);

        // When
        rxSession.lastBookmark();

        // Then
        verify(session).lastBookmarks();
        verifyNoMoreInteractions(session);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDelegateBookmarks() {
        // Given
        NetworkSession session = mock(NetworkSession.class);
        InternalRxSession rxSession = new InternalRxSession(session);

        // When
        rxSession.lastBookmarks();

        // Then
        verify(session).lastBookmarks();
        verifyNoMoreInteractions(session);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDelegateClose() {
        // Given
        NetworkSession session = mock(NetworkSession.class);
        when(session.closeAsync()).thenReturn(completedWithNull());
        InternalRxSession rxSession = new InternalRxSession(session);

        // When
        var mono = rxSession.<Void>close();

        // Then
        StepVerifier.create(mono).verifyComplete();
        verify(session).closeAsync();
        verifyNoMoreInteractions(session);
    }

    @ParameterizedTest
    @MethodSource("executeVariations")
    void shouldDelegateExecuteReadToRetryLogic(ExecuteVariation executeVariation) {
        // GIVEN
        NetworkSession networkSession = mock(NetworkSession.class);
        ReactiveSession session = new InternalReactiveSession(networkSession);
        RetryLogic logic = mock(RetryLogic.class);
        String expected = "";
        given(networkSession.retryLogic()).willReturn(logic);
        ReactiveTransactionCallback<Publisher<String>> tc =
                (ignored) -> publisherToFlowPublisher(Mono.justOrEmpty(expected));
        given(logic.<String>retryRx(any())).willReturn(flowPublisherToFlux(tc.execute(null)));
        TransactionConfig config = TransactionConfig.builder().build();

        // WHEN
        Publisher<String> actual = executeVariation.readOnly
                ? (executeVariation.explicitTxConfig ? session.executeRead(tc, config) : session.executeRead(tc))
                : (executeVariation.explicitTxConfig ? session.executeWrite(tc, config) : session.executeWrite(tc));

        // THEN
        assertEquals(expected, Mono.from(flowPublisherToFlux(actual)).block());
        then(networkSession).should().retryLogic();
        then(logic).should().retryRx(any());
    }

    static List<ExecuteVariation> executeVariations() {
        return Arrays.asList(
                new ExecuteVariation(false, false),
                new ExecuteVariation(false, true),
                new ExecuteVariation(true, false),
                new ExecuteVariation(true, true));
    }

    private static class ExecuteVariation {
        private final boolean readOnly;
        private final boolean explicitTxConfig;

        private ExecuteVariation(boolean readOnly, boolean explicitTxConfig) {
            this.readOnly = readOnly;
            this.explicitTxConfig = explicitTxConfig;
        }
    }
}
