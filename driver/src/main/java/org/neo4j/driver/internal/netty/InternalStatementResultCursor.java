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
package org.neo4j.driver.internal.netty;

import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.internal.handlers.RecordsResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.v1.Record;

public class InternalStatementResultCursor implements StatementResultCursor
{
    private final AsyncConnection connection;
    private final RunResponseHandler runResponseHandler;
    private final RecordsResponseHandler pullAllResponseHandler;

    public InternalStatementResultCursor( AsyncConnection connection )
    {
        this.connection = connection;
        this.runResponseHandler = new RunResponseHandler();
        this.pullAllResponseHandler = new RecordsResponseHandler( runResponseHandler, connection );
    }

    public RunResponseHandler runResponseHandler()
    {
        return runResponseHandler;
    }

    public RecordsResponseHandler pullAllResponseHandler()
    {
        return pullAllResponseHandler;
    }

    @Override
    public ListenableFuture<Boolean> fetchAsync()
    {
        return pullAllResponseHandler.recordAvailable();
    }

    @Override
    public Record current()
    {
        return pullAllResponseHandler.recordBuffer().poll();
    }

    public ListenableFuture<List<Record>> allAsync()
    {
        List<Record> result = new ArrayList<>();
        Promise<List<Record>> resultPromise = connection.newPromise();
        fetchAll( result, resultPromise );
        return new InternalListenableFuture<>( resultPromise );
    }

    private void fetchAll( final List<Record> result, final Promise<List<Record>> resultPromise )
    {
        final ListenableFuture<Boolean> fetch = fetchAsync();
        fetch.addListener( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Boolean available = fetch.get();
                    if ( available )
                    {
                        result.add( current() );
                        fetchAll( result, resultPromise );
                    }
                    else
                    {
                        resultPromise.setSuccess( result );
                    }
                }
                catch ( Throwable t )
                {
                    resultPromise.setFailure( t );
                }
            }
        } );
    }
}
