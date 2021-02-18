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

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Query;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;

public class InternalAsyncTransaction extends AsyncAbstractQueryRunner implements AsyncTransaction
{
    private final UnmanagedTransaction tx;
    public InternalAsyncTransaction( UnmanagedTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public CompletionStage<Void> commitAsync()
    {
        return tx.commitAsync();
    }

    @Override
    public CompletionStage<Void> rollbackAsync()
    {
        return tx.rollbackAsync();
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query)
    {
        return tx.runAsync(query, true );
    }

    public boolean isOpen()
    {
        return tx.isOpen();
    }
}
