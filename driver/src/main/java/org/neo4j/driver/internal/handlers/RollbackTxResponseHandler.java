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
package org.neo4j.driver.internal.handlers;

import io.netty.util.concurrent.Promise;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

public class RollbackTxResponseHandler implements ResponseHandler
{
    private final Promise<Void> rollbackTxPromise;
    private final SessionResourcesHandler resourcesHandler;
    private final ExplicitTransaction tx;

    public RollbackTxResponseHandler( Promise<Void> rollbackTxPromise, SessionResourcesHandler resourcesHandler,
            ExplicitTransaction tx )
    {
        this.rollbackTxPromise = requireNonNull( rollbackTxPromise );
        this.resourcesHandler = requireNonNull( resourcesHandler );
        this.tx = requireNonNull( tx );
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        resourcesHandler.onAsyncTransactionClosed( tx );
        rollbackTxPromise.setSuccess( null );
    }

    @Override
    public void onFailure( Throwable error )
    {
        resourcesHandler.onAsyncTransactionClosed( tx );
        rollbackTxPromise.setFailure( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException(
                "Transaction rollback is not expected to receive records: " + Arrays.toString( fields ) );
    }
}
