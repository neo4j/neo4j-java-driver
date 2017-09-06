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

import org.neo4j.driver.internal.handlers.AsyncRecordsResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.v1.Record;

public class InternalStatementResultCursor implements StatementResultCursor
{
    private final RunResponseHandler runResponseHandler;
    private final AsyncRecordsResponseHandler pullAllResponseHandler;

    public InternalStatementResultCursor( AsyncConnection connection, boolean releaseConnection )
    {
        this.runResponseHandler = new RunResponseHandler();
        this.pullAllResponseHandler =
                new AsyncRecordsResponseHandler( runResponseHandler, connection, releaseConnection );
    }

    public RunResponseHandler runResponseHandler()
    {
        return runResponseHandler;
    }

    public AsyncRecordsResponseHandler pullAllResponseHandler()
    {
        return pullAllResponseHandler;
    }

    @Override
    public Task<Boolean> fetchAsync()
    {
        return pullAllResponseHandler.recordAvailable();
    }

    @Override
    public Record current()
    {
        return pullAllResponseHandler.pollCurrent();
    }
}
