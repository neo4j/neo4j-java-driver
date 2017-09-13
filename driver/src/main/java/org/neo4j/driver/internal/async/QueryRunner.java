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

import java.util.Map;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Function;

import static org.neo4j.driver.v1.Values.ofValue;

public final class QueryRunner
{
    private QueryRunner()
    {
    }

    public static InternalFuture<StatementResultCursor> runAsync( AsyncConnection connection, Statement statement )
    {
        return runAsync( connection, statement, null );
    }

    public static InternalFuture<StatementResultCursor> runAsync( AsyncConnection connection, Statement statement,
            ExplicitTransaction tx )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        InternalPromise<Void> runCompletedPromise = connection.newPromise();
        final RunResponseHandler runHandler = new RunResponseHandler( runCompletedPromise );
        final PullAllResponseHandler pullAllHandler = newPullAllHandler( runHandler, connection, tx );

        connection.run( query, params, runHandler );
        connection.pullAll( pullAllHandler );
        connection.flush();

        return runCompletedPromise.thenApply( new Function<Void,StatementResultCursor>()
        {
            @Override
            public StatementResultCursor apply( Void ignore )
            {
                return new InternalStatementResultCursor( runHandler, pullAllHandler );
            }
        } );
    }

    private static PullAllResponseHandler newPullAllHandler( RunResponseHandler runHandler, AsyncConnection connection,
            ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( runHandler, connection, tx );
        }
        return new SessionPullAllResponseHandler( runHandler, connection );
    }
}
