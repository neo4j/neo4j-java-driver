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

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Predicate.isEqual;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.values;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.summary.ResultSummary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SuppressWarnings("deprecation")
class InternalRxResultTest {
    @Test
    void shouldInitCursorFuture() {
        // Given
        RxResultCursor cursor = mock(RxResultCursorImpl.class);
        var rxResult = newRxResult(cursor);

        // When
        var cursorFuture = rxResult.initCursorFuture().toCompletableFuture();

        // Then
        assertTrue(cursorFuture.isDone());
        assertThat(Futures.getNow(cursorFuture), equalTo(cursor));
    }

    @Test
    void shouldInitCursorFutureWithFailedCursor() {
        // Given
        var error = new RuntimeException("Failed to obtain cursor probably due to connection problem");
        var rxResult = newRxResult(error);

        // When
        var cursorFuture = rxResult.initCursorFuture().toCompletableFuture();

        // Then
        assertTrue(cursorFuture.isDone());
        var actualError = assertThrows(RuntimeException.class, () -> Futures.getNow(cursorFuture));
        assertThat(actualError.getCause(), equalTo(error));
    }

    @Test
    void shouldObtainKeys() {
        // Given
        RxResultCursor cursor = mock(RxResultCursorImpl.class);
        RxResult rxResult = newRxResult(cursor);

        var keys = Arrays.asList("one", "two", "three");
        when(cursor.keys()).thenReturn(keys);

        // When & Then
        StepVerifier.create(Flux.from(rxResult.keys()))
                .expectNext(Arrays.asList("one", "two", "three"))
                .verifyComplete();
    }

    @Test
    void shouldErrorWhenFailedObtainKeys() {
        // Given
        var error = new RuntimeException("Failed to obtain cursor");
        var rxResult = newRxResult(error);

        // When & Then
        StepVerifier.create(Flux.from(rxResult.keys()))
                .expectErrorMatches(isEqual(error))
                .verify();
    }

    @Test
    void shouldCancelKeys() {
        // Given
        RxResultCursor cursor = mock(RxResultCursorImpl.class);
        RxResult rxResult = newRxResult(cursor);

        var keys = Arrays.asList("one", "two", "three");
        when(cursor.keys()).thenReturn(keys);

        // When & Then
        StepVerifier.create(Flux.from(rxResult.keys()).limitRate(1).take(1))
                .expectNext(Arrays.asList("one", "two", "three"))
                .verifyComplete();
    }

    @Test
    void shouldObtainRecordsAndSummary() {
        // Given
        var boltConnection = mock(BoltConnection.class);
        given(boltConnection.pull(anyLong(), anyLong())).willReturn(CompletableFuture.completedFuture(boltConnection));
        given(boltConnection.serverAddress()).willReturn(new BoltServerAddress("localhost"));
        given(boltConnection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 1));
        given(boltConnection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArguments()[0];
            handler.onRecord(values(1, 1, 1));
            handler.onRecord(values(2, 2, 2));
            handler.onRecord(values(3, 3, 3));
            handler.onPullSummary(mock());
            return CompletableFuture.completedFuture(null);
        });
        var runSummary = mock(RunSummary.class);
        given(runSummary.keys()).willReturn(List.of("key1", "key2", "key3"));
        Record record1 = new InternalRecord(asList("key1", "key2", "key3"), values(1, 1, 1));
        Record record2 = new InternalRecord(asList("key1", "key2", "key3"), values(2, 2, 2));
        Record record3 = new InternalRecord(asList("key1", "key2", "key3"), values(3, 3, 3));

        RxResult rxResult = newRxResult(boltConnection, runSummary);

        // When
        StepVerifier.create(Flux.from(rxResult.records()))
                .expectNext(record1)
                .expectNext(record2)
                .expectNext(record3)
                .verifyComplete();
        StepVerifier.create(Mono.from(rxResult.consume())).expectNextCount(1).verifyComplete();
    }

    @Test
    void shouldCancelStreamingButObtainSummary() {
        // Given
        var boltConnection = mock(BoltConnection.class);
        given(boltConnection.pull(anyLong(), anyLong())).willReturn(CompletableFuture.completedFuture(boltConnection));
        given(boltConnection.serverAddress()).willReturn(new BoltServerAddress("localhost"));
        given(boltConnection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 1));
        given(boltConnection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArguments()[0];
            handler.onRecord(values(1, 1, 1));
            handler.onRecord(values(2, 2, 2));
            handler.onRecord(values(3, 3, 3));
            handler.onPullSummary(mock());
            return CompletableFuture.completedFuture(null);
        });
        var runSummary = mock(RunSummary.class);
        given(runSummary.keys()).willReturn(List.of("key1", "key2", "key3"));
        Record record1 = new InternalRecord(asList("key1", "key2", "key3"), values(1, 1, 1));

        RxResult rxResult = newRxResult(boltConnection, runSummary);

        // When
        StepVerifier.create(Flux.from(rxResult.records()).limitRate(1).take(1))
                .expectNext(record1)
                .verifyComplete();
        StepVerifier.create(Mono.from(rxResult.consume())).expectNextCount(1).verifyComplete();
    }

    @Test
    void shouldErrorIfFailedToCreateCursor() {
        // Given
        Throwable error = new RuntimeException("Hi");
        RxResult rxResult = newRxResult(error);

        // When & Then
        StepVerifier.create(Flux.from(rxResult.records()))
                .expectErrorMatches(isEqual(error))
                .verify();
        StepVerifier.create(Mono.from(rxResult.consume()))
                .expectErrorMatches(isEqual(error))
                .verify();
    }

    @Test
    void shouldErrorIfFailedToStream() {
        // Given
        var boltConnection = mock(BoltConnection.class);
        given(boltConnection.pull(anyLong(), anyLong())).willReturn(CompletableFuture.completedFuture(boltConnection));
        given(boltConnection.serverAddress()).willReturn(new BoltServerAddress("localhost"));
        given(boltConnection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 1));
        Throwable error = new RuntimeException("Hi");
        given(boltConnection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArguments()[0];
            handler.onError(error);
            return CompletableFuture.completedFuture(null);
        });
        RxResult rxResult = newRxResult(boltConnection);

        // When & Then
        StepVerifier.create(Flux.from(rxResult.records()))
                .expectErrorMatches(isEqual(error))
                .verify();
        StepVerifier.create(Mono.from(rxResult.consume()))
                .assertNext(summary -> assertThat(summary, instanceOf(ResultSummary.class)))
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDelegateIsOpen(boolean expectedState) {
        // Given
        var cursor = mock(RxResultCursor.class);
        given(cursor.isDone()).willReturn(!expectedState);
        RxResult result = new InternalRxResult(() -> CompletableFuture.completedFuture(cursor));

        // When
        var actualState = Mono.from(result.isOpen()).block();

        // Then
        assertEquals(expectedState, actualState);
        then(cursor).should().isDone();
    }

    private InternalRxResult newRxResult(BoltConnection boltConnection) {
        return newRxResult(boltConnection, mock());
    }

    private InternalRxResult newRxResult(BoltConnection boltConnection, RunSummary runSummary) {
        RxResultCursor cursor = new RxResultCursorImpl(
                boltConnection,
                mock(),
                runSummary,
                null,
                () -> null,
                databaseBookmark -> {},
                throwable -> {},
                false,
                () -> null);
        return newRxResult(cursor);
    }

    private InternalRxResult newRxResult(RxResultCursor cursor) {
        return new InternalRxResult(() -> {
            // now we successfully run
            return completedFuture(cursor);
        });
    }

    private InternalRxResult newRxResult(Throwable error) {
        return new InternalRxResult(() -> {
            // now we successfully run
            return failedFuture(new CompletionException(error));
        });
    }
}
