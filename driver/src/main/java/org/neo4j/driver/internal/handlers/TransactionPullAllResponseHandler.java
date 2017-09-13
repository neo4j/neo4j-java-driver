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

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.v1.Statement;

import static java.util.Objects.requireNonNull;

public class TransactionPullAllResponseHandler extends PullAllResponseHandler
{
    private final ExplicitTransaction tx;

    public TransactionPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler,
            AsyncConnection connection, ExplicitTransaction tx )
    {
        super( statement, runResponseHandler, connection );
        this.tx = requireNonNull( tx );
    }

    @Override
    protected void afterSuccess()
    {
    }

    @Override
    protected void afterFailure( Throwable error )
    {
        tx.resultFailed( error );
    }
}
