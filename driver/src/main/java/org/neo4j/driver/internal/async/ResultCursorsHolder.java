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
package org.neo4j.driver.internal.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.FailableCursor;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class ResultCursorsHolder
{
    private final List<CompletionStage<? extends FailableCursor>> cursorStages = Collections.synchronizedList( new ArrayList<>() );

    public void add( CompletionStage<? extends FailableCursor> cursorStage )
    {
        Objects.requireNonNull( cursorStage );
        cursorStages.add( cursorStage );
    }

    CompletionStage<Throwable> retrieveNotConsumedError()
    {
        CompletableFuture<Throwable>[] failures = retrieveAllFailures();

        return CompletableFuture.allOf( failures )
                .thenApply( ignore -> findFirstFailure( failures ) );
    }

    @SuppressWarnings( "unchecked" )
    private CompletableFuture<Throwable>[] retrieveAllFailures()
    {
        return cursorStages.stream()
                .map( ResultCursorsHolder::retrieveFailure )
                .map( CompletionStage::toCompletableFuture )
                .toArray( CompletableFuture[]::new );
    }

    private static Throwable findFirstFailure( CompletableFuture<Throwable>[] completedFailureFutures )
    {
        // all given futures should be completed, it is thus safe to get their values

        for ( CompletableFuture<Throwable> failureFuture : completedFailureFutures )
        {
            Throwable failure = failureFuture.getNow( null ); // does not block
            if ( failure != null )
            {
                return failure;
            }
        }
        return null;
    }

    private static CompletionStage<Throwable> retrieveFailure( CompletionStage<? extends FailableCursor> cursorStage )
    {
        return cursorStage
                .exceptionally( cursor -> null )
                .thenCompose( cursor -> cursor == null ? completedWithNull() : cursor.discardAllFailureAsync() );
    }
}
