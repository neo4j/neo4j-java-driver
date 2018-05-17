/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

public class ResetResponseHandler implements ResponseHandler
{
    private final InboundMessageDispatcher messageDispatcher;
    private final CompletableFuture<Void> completionFuture;

    public ResetResponseHandler( InboundMessageDispatcher messageDispatcher, CompletableFuture<Void> completionFuture )
    {
        this.messageDispatcher = messageDispatcher;
        this.completionFuture = completionFuture;
    }

    @Override
    public final void onSuccess( Map<String,Value> metadata )
    {
        resetCompleted();
    }

    @Override
    public final void onFailure( Throwable error )
    {
        resetCompleted();
    }

    @Override
    public final void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    private void resetCompleted()
    {
        messageDispatcher.unMuteAckFailure();
        resetCompleted( completionFuture );
    }

    protected void resetCompleted( CompletableFuture<Void> completionFuture )
    {
        completionFuture.complete( null );
    }
}
