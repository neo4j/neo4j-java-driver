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
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;

import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

@SuppressWarnings("deprecation")
class InternalRxTransactionTest {
    @Test
    void commitShouldDelegate() {
        var tx = mock(UnmanagedTransaction.class);
        when(tx.commitAsync()).thenReturn(Futures.completedWithNull());

        var rxTx = new InternalRxTransaction(tx);
        Publisher<Void> publisher = rxTx.commit();
        StepVerifier.create(publisher).verifyComplete();

        verify(tx).commitAsync();
    }

    @Test
    void rollbackShouldDelegate() {
        var tx = mock(UnmanagedTransaction.class);
        when(tx.rollbackAsync()).thenReturn(Futures.completedWithNull());

        var rxTx = new InternalRxTransaction(tx);
        Publisher<Void> publisher = rxTx.rollback();
        StepVerifier.create(publisher).verifyComplete();

        verify(tx).rollbackAsync();
    }

    private static Stream<Function<RxTransaction, RxResult>> allTxRunMethods() {
        return Stream.of(
                rxSession -> rxSession.run("RETURN 1"),
                rxSession -> rxSession.run("RETURN $x", parameters("x", 1)),
                rxSession -> rxSession.run("RETURN $x", singletonMap("x", 1)),
                rxSession -> rxSession.run(
                        "RETURN $x", new InternalRecord(singletonList("x"), new Value[] {new IntegerValue(1)})),
                rxSession -> rxSession.run(new Query("RETURN $x", parameters("x", 1))));
    }

    @ParameterizedTest
    @MethodSource("allTxRunMethods")
    void shouldDelegateRun(Function<RxTransaction, RxResult> runReturnOne) {
        // Given
        var tx = mock(UnmanagedTransaction.class);
        RxResultCursor cursor = mock(RxResultCursorImpl.class);

        // Run succeeded with a cursor
        when(tx.runRx(any(Query.class))).thenReturn(completedFuture(cursor));
        var rxTx = new InternalRxTransaction(tx);

        // When
        var result = runReturnOne.apply(rxTx);
        // Execute the run
        var cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify(tx).runRx(any(Query.class));
        assertThat(Futures.getNow(cursorFuture), equalTo(cursor));
    }

    @ParameterizedTest
    @MethodSource("allTxRunMethods")
    void shouldMarkTxIfFailedToRun(Function<RxTransaction, RxResult> runReturnOne) {
        // Given
        Throwable error = new RuntimeException("Hi there");
        var tx = mock(UnmanagedTransaction.class);

        // Run failed with error
        when(tx.runRx(any(Query.class))).thenReturn(failedFuture(error));
        var rxTx = new InternalRxTransaction(tx);

        // When
        var result = runReturnOne.apply(rxTx);
        // Execute the run
        var cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify(tx).runRx(any(Query.class));
        RuntimeException t = assertThrows(CompletionException.class, () -> Futures.getNow(cursorFuture));
        assertThat(t.getCause(), equalTo(error));
        verify(tx).markTerminated(error);
    }

    @Test
    void shouldDelegateConditionalClose() {
        var tx = mock(UnmanagedTransaction.class);
        when(tx.closeAsync(true)).thenReturn(Futures.completedWithNull());

        var rxTx = new InternalRxTransaction(tx);
        var publisher = rxTx.close(true);
        StepVerifier.create(publisher).verifyComplete();

        verify(tx).closeAsync(true);
    }

    @Test
    void shouldDelegateClose() {
        var tx = mock(UnmanagedTransaction.class);
        when(tx.closeAsync(false)).thenReturn(Futures.completedWithNull());

        var rxTx = new InternalRxTransaction(tx);
        var publisher = rxTx.close();
        StepVerifier.create(publisher).verifyComplete();

        verify(tx).closeAsync(false);
    }

    @Test
    void shouldDelegateIsOpenAsync() {
        // GIVEN
        var utx = mock(UnmanagedTransaction.class);
        var expected = false;
        given(utx.isOpen()).willReturn(expected);
        RxTransaction tx = new InternalRxTransaction(utx);

        // WHEN & THEN
        StepVerifier.create(tx.isOpen()).expectNext(expected).expectComplete().verify();
        then(utx).should().isOpen();
    }
}
