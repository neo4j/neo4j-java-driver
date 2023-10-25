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
package org.neo4j.driver.internal.handlers.pulln;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.DEFAULT_FETCH_SIZE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.handlers.PullAllResponseHandlerTestBase;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.BooleanValue;

class AutoPullResponseHandlerTest extends PullAllResponseHandlerTestBase<AutoPullResponseHandler> {
    @Override
    protected AutoPullResponseHandler newHandler(Query query, List<String> queryKeys, Connection connection) {
        var runResponseHandler = new RunResponseHandler(
                new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR, mock(Connection.class), null);
        runResponseHandler.onSuccess(singletonMap("fields", value(queryKeys)));
        var handler = new AutoPullResponseHandler(
                query,
                runResponseHandler,
                connection,
                BoltProtocolV3.METADATA_EXTRACTOR,
                mock(PullResponseCompletionListener.class),
                DEFAULT_FETCH_SIZE);
        handler.prePopulateRecords();
        return handler;
    }

    protected AutoPullResponseHandler newHandler(Query query, Connection connection, long fetchSize) {
        var runResponseHandler = new RunResponseHandler(
                new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR, mock(Connection.class), null);
        runResponseHandler.onSuccess(emptyMap());
        var handler = new AutoPullResponseHandler(
                query,
                runResponseHandler,
                connection,
                BoltProtocolV3.METADATA_EXTRACTOR,
                mock(PullResponseCompletionListener.class),
                fetchSize);
        handler.prePopulateRecords();
        return handler;
    }

    @Test
    void shouldKeepRequestingWhenBetweenRange() {
        var connection = connectionMock();
        var inOrder = Mockito.inOrder(connection);

        // highwatermark=2, lowwatermark=1
        var handler = newHandler(new Query("RETURN 1"), connection, 4);

        Map<String, Value> metaData = new HashMap<>(1);
        metaData.put("has_more", BooleanValue.TRUE);

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());

        handler.onRecord(values(1));
        handler.onRecord(values(2));
        handler.onSuccess(metaData); // 2 in the record queue

        // should send another pulln request since maxValue not met
        inOrder.verify(connection).writeAndFlush(any(), any());
    }

    @Test
    void shouldStopRequestingWhenOverMaxWatermark() {
        var connection = connectionMock();
        var inOrder = Mockito.inOrder(connection);

        // highWatermark=2, lowWatermark=1
        var handler = newHandler(new Query("RETURN 1"), connection, 4);

        Map<String, Value> metaData = new HashMap<>(1);
        metaData.put("has_more", BooleanValue.TRUE);

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());

        handler.onRecord(values(1));
        handler.onRecord(values(2));
        handler.onRecord(values(3));
        handler.onSuccess(metaData);

        // only initial writeAndFlush()
        verify(connection, times(1)).writeAndFlush(any(PullMessage.class), any());
    }

    @Test
    void shouldRestartRequestingWhenMinimumWatermarkMet() {
        var connection = connectionMock();
        var inOrder = Mockito.inOrder(connection);

        // highwatermark=4, lowwatermark=2
        var handler = newHandler(new Query("RETURN 1"), connection, 7);

        Map<String, Value> metaData = new HashMap<>(1);
        metaData.put("has_more", BooleanValue.TRUE);

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());

        handler.onRecord(values(1));
        handler.onRecord(values(2));
        handler.onRecord(values(3));
        handler.onRecord(values(4));
        handler.onRecord(values(5));
        handler.onSuccess(metaData);

        verify(connection, times(1)).writeAndFlush(any(PullMessage.class), any());

        handler.nextAsync();
        handler.nextAsync();
        handler.nextAsync();

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());
    }

    @Test
    void shouldKeepRequestingMoreRecordsWhenPullAll() {
        var connection = connectionMock();
        var handler = newHandler(new Query("RETURN 1"), connection, -1);

        Map<String, Value> metaData = new HashMap<>(1);
        metaData.put("has_more", BooleanValue.TRUE);

        handler.onRecord(values(1));
        handler.onSuccess(metaData);

        handler.onRecord(values(2));
        handler.onSuccess(metaData);

        handler.onRecord(values(3));
        handler.onSuccess(emptyMap());

        verify(connection, times(3)).writeAndFlush(any(PullMessage.class), any());
    }

    @Test
    void shouldFunctionWhenHighAndLowWatermarksAreEqual() {
        var connection = connectionMock();
        var inOrder = Mockito.inOrder(connection);

        // highwatermark=0, lowwatermark=0
        var handler = newHandler(new Query("RETURN 1"), connection, 1);

        Map<String, Value> metaData = new HashMap<>(1);
        metaData.put("has_more", BooleanValue.TRUE);

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());

        handler.onRecord(values(1));
        handler.onSuccess(metaData);

        inOrder.verify(connection, never()).writeAndFlush(any(), any());

        handler.nextAsync();

        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());
    }
}
