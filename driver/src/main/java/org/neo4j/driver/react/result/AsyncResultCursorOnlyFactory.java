/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.react.result;

import org.neo4j.driver.internal.LegacyInternalStatementResultCursor;
import org.neo4j.driver.internal.handlers.AbstractPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * Used by Bolt V1, V2, V3
 */
public class AsyncResultCursorOnlyFactory implements StatementResultCursorFactory
{
    private final RunResponseHandler runHandler;
    private final AbstractPullAllResponseHandler pullAllHandler;

    public AsyncResultCursorOnlyFactory( RunResponseHandler runHandler, AbstractPullAllResponseHandler pullAllHandler )
    {
        this.runHandler = runHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public InternalStatementResultCursor asyncResult()
    {
        return new LegacyInternalStatementResultCursor( runHandler, pullAllHandler );
    }

    @Override
    public RxStatementResultCursor rxResult()
    {
        throw new ClientException( "Driver is connected to the database that does not support reactive API. " +
                "In order to use the reactive API, please upgrade to neo4j 4.0.0 or later." );
    }
}
