/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.stress;

import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

@ParallelizableIT
class SingleInstanceStressIT extends AbstractStressTestBase<SingleInstanceStressIT.Context>
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Override
    URI databaseUri()
    {
        return neo4j.uri();
    }

    @Override
    AuthToken authToken()
    {
        return neo4j.authToken();
    }

    @Override
    Config.ConfigBuilder config( Config.ConfigBuilder builder )
    {
        return builder.withoutEncryption();
    }

    @Override
    Context createContext()
    {
        return new Context();
    }

    @Override
    boolean handleWriteFailure( Throwable error, Context context )
    {
        // no write failures expected
        return false;
    }

    @Override
    <A extends Context> void printStats( A context )
    {
        System.out.println( "Nodes read: " + context.getReadNodesCount() );
        System.out.println( "Nodes created: " + context.getCreatedNodesCount() );

        System.out.println( "Bookmark failures: " + context.getBookmarkFailures() );
    }

    @Override
    List<BlockingCommand<Context>> createTestSpecificBlockingCommands()
    {
        return Arrays.asList(
                new BlockingReadQuery<>( driver, false ),
                new BlockingReadQuery<>( driver, true ),

                new BlockingReadQueryInTx<>( driver, false ),
                new BlockingReadQueryInTx<>( driver, true ),

                new BlockingWriteQuery<>( this, driver, false ),
                new BlockingWriteQuery<>( this, driver, true ),

                new BlockingWriteQueryInTx<>( this, driver, false ),
                new BlockingWriteQueryInTx<>( this, driver, true ),

                new BlockingWrongQuery<>( driver ),
                new BlockingWrongQueryInTx<>( driver ),

                new BlockingFailingQuery<>( driver ),
                new BlockingFailingQueryInTx<>( driver ) );
    }

    @Override
    List<AsyncCommand<Context>> createTestSpecificAsyncCommands()
    {
        return Arrays.asList(
                new AsyncReadQuery<>( driver, false ),
                new AsyncReadQuery<>( driver, true ),

                new AsyncReadQueryInTx<>( driver, false ),
                new AsyncReadQueryInTx<>( driver, true ),

                new AsyncWriteQuery<>( this, driver, false ),
                new AsyncWriteQuery<>( this, driver, true ),

                new AsyncWriteQueryInTx<>( this, driver, false ),
                new AsyncWriteQueryInTx<>( this, driver, true ),

                new AsyncWrongQuery<>( driver ),
                new AsyncWrongQueryInTx<>( driver ),

                new AsyncFailingQuery<>( driver ),
                new AsyncFailingQueryInTx<>( driver ) );
    }

    @Override
    List<RxCommand<Context>> createTestSpecificRxCommands()
    {
        return Arrays.asList(
                new RxReadQuery<>( driver, false ),
                new RxReadQuery<>( driver, true ),

                new RxWriteQuery<>( this, driver, false ),
                new RxWriteQuery<>( this, driver, true ),

                new RxReadQueryInTx<>( driver, false ),
                new RxReadQueryInTx<>( driver, true ),

                new RxWriteQueryInTx<>( this, driver, false ),
                new RxWriteQueryInTx<>( this, driver, true ),

                new RxFailingQuery<>( driver ),
                new RxFailingQueryInTx<>( driver )
        );
    }

    static class Context extends AbstractContext
    {
    }

    @Override
    void dumpLogs()
    {
        neo4j.dumpLogs();
    }
}
