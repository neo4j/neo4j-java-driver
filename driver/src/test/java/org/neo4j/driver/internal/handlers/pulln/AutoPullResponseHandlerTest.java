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
package org.neo4j.driver.internal.handlers.pulln;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Query;
import org.neo4j.driver.internal.handlers.PullAllResponseHandlerTestBase;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.DEFAULT_FETCH_SIZE;
import static org.neo4j.driver.internal.messaging.v1.BoltProtocolV1.METADATA_EXTRACTOR;

class AutoPullResponseHandlerTest extends PullAllResponseHandlerTestBase<AutoPullResponseHandler>
{
    @Override
    protected AutoPullResponseHandler newHandler(Query query, List<String> queryKeys, Connection connection )
    {
        RunResponseHandler runResponseHandler = new RunResponseHandler( new CompletableFuture<>(), METADATA_EXTRACTOR );
        runResponseHandler.onSuccess( singletonMap( "fields", value(queryKeys) ) );
        AutoPullResponseHandler handler =
                new AutoPullResponseHandler(query, runResponseHandler, connection, METADATA_EXTRACTOR, mock( PullResponseCompletionListener.class ),
                        DEFAULT_FETCH_SIZE );
        handler.prePopulateRecords();
        return handler;
    }
}
