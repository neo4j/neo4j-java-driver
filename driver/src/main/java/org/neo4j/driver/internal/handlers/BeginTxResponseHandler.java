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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.Value;

import static java.util.Objects.requireNonNull;

public class BeginTxResponseHandler implements ResponseHandler
{
    private final CompletableFuture<Void> beginTxFuture;

    public BeginTxResponseHandler( CompletableFuture<Void> beginTxFuture )
    {
        this.beginTxFuture = requireNonNull( beginTxFuture );
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        beginTxFuture.complete( null );
    }

    @Override
    public void onFailure( Throwable error )
    {
        beginTxFuture.completeExceptionally( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException(
                "Transaction begin is not expected to receive records: " + Arrays.toString( fields ) );
    }
}
