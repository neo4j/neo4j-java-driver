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
package org.neo4j.driver.internal.handlers;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.summary.ResultSummary;

public interface PullAllResponseHandler extends ResponseHandler
{
    CompletionStage<ResultSummary> consumeAsync();

    CompletionStage<Record> nextAsync();

    CompletionStage<Record> peekAsync();

    <T> CompletionStage<List<T>> listAsync( Function<Record, T> mapFunction );

    CompletionStage<Throwable> pullAllFailureAsync();

    void prePopulateRecords();
}
