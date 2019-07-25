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

import org.neo4j.driver.Statement;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.SessionPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.TransactionPullResponseHandler;
import org.neo4j.driver.internal.messaging.v1.BoltProtocolV1;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;

public class PullHandlers
{
    public static AbstractPullAllResponseHandler newBoltV1PullAllHandler( Statement statement, RunResponseHandler runHandler,
            Connection connection, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx, BoltProtocolV1.METADATA_EXTRACTOR );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection, BookmarkHolder.NO_OP, BoltProtocolV1.METADATA_EXTRACTOR );
    }

    public static AbstractPullAllResponseHandler newBoltV3PullAllHandler( Statement statement, RunResponseHandler runHandler, Connection connection,
            BookmarkHolder bookmarkHolder, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx, BoltProtocolV3.METADATA_EXTRACTOR );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection, bookmarkHolder, BoltProtocolV3.METADATA_EXTRACTOR );
    }

    public static BasicPullResponseHandler newBoltV4PullHandler( Statement statement, RunResponseHandler runHandler, Connection connection,
            BookmarkHolder bookmarkHolder, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullResponseHandler( statement, runHandler, connection, tx, BoltProtocolV3.METADATA_EXTRACTOR );
        }
        return new SessionPullResponseHandler( statement, runHandler, connection, bookmarkHolder, BoltProtocolV3.METADATA_EXTRACTOR );
    }

}
