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
package org.neo4j.driver.internal.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;

class RxResultCursorImplTest {
    @Mock
    DriverBoltConnection connection;

    @Mock
    Query query;

    @Mock
    RunSummary runSummary;

    @Mock
    Consumer<DatabaseBookmark> bookmarkConsumer;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 5));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldNotifyRecordConsumerOfRunError(boolean getRunError) {
        // given
        var runError = mock(Throwable.class);
        given(connection.serverAddress()).willReturn(new BoltServerAddress("localhost"));
        @SuppressWarnings("deprecation")
        var cursor = new RxResultCursorImpl(connection, query, null, runError, bookmarkConsumer, false, Logging.none());
        if (getRunError) {
            assertEquals(runError, cursor.getRunError());
        }
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);

        // when
        cursor.installRecordConsumer(recordConsumer);

        // then
        then(recordConsumer).should().accept(null, runError);
        assertNotNull(cursor.summaryAsync().toCompletableFuture().join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReturnSummaryWithRunError(boolean getRunError) {
        // given
        var runError = mock(Throwable.class);
        given(connection.serverAddress()).willReturn(new BoltServerAddress("localhost"));
        @SuppressWarnings("deprecation")
        var cursor = new RxResultCursorImpl(connection, query, null, runError, bookmarkConsumer, false, Logging.none());
        if (getRunError) {
            assertEquals(runError, cursor.getRunError());
        }

        // when
        var summary = cursor.summaryAsync().toCompletableFuture();

        // then
        assertEquals(
                runError, assertThrows(CompletionException.class, summary::join).getCause());
    }

    @Test
    void shouldReturnKeys() {
        // given
        var keys = List.of("a", "b");
        given(runSummary.keys()).willReturn(keys);
        @SuppressWarnings("deprecation")
        var cursor =
                new RxResultCursorImpl(connection, query, runSummary, null, bookmarkConsumer, false, Logging.none());

        // when & then
        assertEquals(keys, cursor.keys());
        then(runSummary).should().keys();
    }
}
