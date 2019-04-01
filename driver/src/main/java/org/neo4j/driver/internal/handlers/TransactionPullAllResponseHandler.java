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
package org.neo4j.driver.internal.handlers;

import java.util.Map;

import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;

import static java.util.Objects.requireNonNull;

public class TransactionPullAllResponseHandler extends AbstractPullAllResponseHandler
{
    private final ExplicitTransaction tx;

    public TransactionPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler,
            Connection connection, ExplicitTransaction tx, MetadataExtractor metadataExtractor )
    {
        super( statement, runResponseHandler, connection, metadataExtractor );
        this.tx = requireNonNull( tx );
    }

    @Override
    protected void afterSuccess( Map<String,Value> metadata )
    {
    }

    @Override
    protected void afterFailure( Throwable error )
    {
        // always mark transaction as terminated because every error is "acknowledged" with a RESET message
        // so database forgets about the transaction after the first error
        // such transaction should not attempt to commit and can be considered as rolled back
        tx.markTerminated();
    }
}
