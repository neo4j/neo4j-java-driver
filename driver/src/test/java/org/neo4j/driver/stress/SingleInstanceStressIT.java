/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    Context createContext()
    {
        return new Context( neo4j.address().toString() );
    }

    @Override
    List<BlockingCommand<Context>> createTestSpecificBlockingCommands()
    {
        return Collections.emptyList();
    }

    @Override
    boolean handleWriteFailure( Throwable error, Context context )
    {
        // no write failures expected
        return false;
    }

    @Override
    void assertExpectedReadQueryDistribution( Context context )
    {
        assertThat( context.getReadQueryCount(), greaterThan( 0L ) );
    }

    @Override
    <A extends Context> void printStats( A context )
    {
        System.out.println( "Nodes read: " + context.getReadNodesCount() );
        System.out.println( "Nodes created: " + context.getCreatedNodesCount() );

        System.out.println( "Bookmark failures: " + context.getBookmarkFailures() );
    }

    static class Context extends AbstractContext
    {
        final String expectedAddress;
        final AtomicLong readQueries = new AtomicLong();

        Context( String expectedAddress )
        {
            this.expectedAddress = expectedAddress;
        }

        @Override
        public void processSummary( ResultSummary summary )
        {
            if ( summary == null )
            {
                return;
            }

            String address = summary.server().address();
            assertEquals( expectedAddress, address );
            readQueries.incrementAndGet();
        }

        long getReadQueryCount()
        {
            return readQueries.get();
        }
    }

    @Override
    void dumpLogs()
    {
        neo4j.dumpLogs();
    }
}
