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
package org.neo4j.driver.internal;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.ResultSummary;

public class InternalResult implements Result {
    private final BoltConnection connection;
    private final ResultCursor cursor;

    public InternalResult(BoltConnection connection, ResultCursor cursor) {
        this.connection = connection;
        this.cursor = cursor;
    }

    @Override
    public List<String> keys() {
        return cursor.keys();
    }

    @Override
    public boolean hasNext() {
        return blockingGet(cursor.peekAsync()) != null;
    }

    @Override
    public Record next() {
        var record = blockingGet(cursor.nextAsync());
        if (record == null) {
            throw new NoSuchRecordException("No more records");
        }
        return record;
    }

    @Override
    public Record single() {
        return blockingGet(cursor.singleAsync());
    }

    @Override
    public Record peek() {
        var record = blockingGet(cursor.peekAsync());
        if (record == null) {
            throw new NoSuchRecordException("Cannot peek past the last record");
        }
        return record;
    }

    @Override
    public Stream<Record> stream() {
        var spliterator = Spliterators.spliteratorUnknownSize(this, Spliterator.IMMUTABLE | Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public List<Record> list() {
        return blockingGet(cursor.listAsync());
    }

    @Override
    public <T> List<T> list(Function<Record, T> mapFunction) {
        return blockingGet(cursor.listAsync(mapFunction));
    }

    @Override
    public ResultSummary consume() {
        return blockingGet(cursor.consumeAsync());
    }

    @Override
    public boolean isOpen() {
        return blockingGet(cursor.isOpenAsync());
    }

    @Override
    public void remove() {
        var message = "Removing records from a result is not supported.";
        throw new ClientException(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }

    private <T> T blockingGet(CompletionStage<T> stage) {
        return Futures.blockingGet(stage, this::terminateConnectionOnThreadInterrupt);
    }

    private void terminateConnectionOnThreadInterrupt() {
        connection.close();
    }
}
