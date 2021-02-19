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
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.util.cc.LocalOrRemoteClusterExtension;

class CausalClusteringStressIT extends AbstractStressTestBase<CausalClusteringStressIT.Context>
{
    @RegisterExtension
    static final LocalOrRemoteClusterExtension clusterRule = new LocalOrRemoteClusterExtension();

    @Override
    URI databaseUri()
    {
        return clusterRule.getClusterUri();
    }

    @Override
    AuthToken authToken()
    {
        return clusterRule.getAuthToken();
    }

    @Override
    Config.ConfigBuilder config( Config.ConfigBuilder builder )
    {
        return clusterRule.config( builder );
    }

    @Override
    Context createContext()
    {
        return new Context();
    }

    @Override
    boolean handleWriteFailure( Throwable error, Context context )
    {
        if ( error instanceof SessionExpiredException )
        {
            boolean isLeaderSwitch = error.getMessage().endsWith( "no longer accepts writes" );
            if ( isLeaderSwitch )
            {
                context.leaderSwitch();
                return true;
            }
        }
        return false;
    }

    @Override
    void printStats( Context context )
    {
        System.out.println( "Nodes read: " + context.getReadNodesCount() );
        System.out.println( "Nodes created: " + context.getCreatedNodesCount() );

        System.out.println( "Leader switches: " + context.getLeaderSwitchCount() );
        System.out.println( "Bookmark failures: " + context.getBookmarkFailures() );
    }

    @Override
    void dumpLogs()
    {
        clusterRule.dumpClusterLogs();
    }

    @Override
    List<BlockingCommand<Context>> createTestSpecificBlockingCommands()
    {
        return Arrays.asList(
                new BlockingWriteQueryUsingReadSessionWithRetries<>( driver, false ),
                new BlockingWriteQueryUsingReadSessionWithRetries<>( driver, true ) );
    }

    static class Context extends AbstractContext
    {
        final AtomicInteger leaderSwitches = new AtomicInteger();

        void leaderSwitch()
        {
            leaderSwitches.incrementAndGet();
        }

        int getLeaderSwitchCount()
        {
            return leaderSwitches.get();
        }
    }

}
