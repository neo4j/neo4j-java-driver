/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.InternalStatementResultCursor;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class ResultCursorsHolder
{
    private final List<CompletionStage<InternalStatementResultCursor>> cursorStages = new ArrayList<>();

    public void add( CompletionStage<InternalStatementResultCursor> cursorStage )
    {
        Objects.requireNonNull( cursorStage );
        cursorStages.add( cursorStage );
    }

    public CompletionStage<Throwable> retrieveNotConsumedError()
    {
        return cursorStages.stream()
                .map( this::retrieveFailure )
                .reduce( completedWithNull(), this::nonNullFailureFromEither );
    }

    private CompletionStage<Throwable> retrieveFailure( CompletionStage<InternalStatementResultCursor> cursorStage )
    {
        return cursorStage
                .exceptionally( cursor -> null )
                .thenCompose( cursor -> cursor == null ? completedWithNull() : cursor.failureAsync() );
    }

    private CompletionStage<Throwable> nonNullFailureFromEither( CompletionStage<Throwable> stage1,
            CompletionStage<Throwable> stage2 )
    {
        return stage1.thenCompose( value ->
        {
            if ( value != null )
            {
                return completedFuture( value );
            }
            return stage2;
        } );
    }
}
