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
package org.neo4j.driver.internal;

import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.util.StubServer;
import org.neo4j.driver.util.StubServerController;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.StubServer.INSECURE_CONFIG;
import static org.neo4j.driver.util.TestUtil.await;

class DirectDriverBoltKitIT
{
    private static StubServerController stubController;

    @BeforeAll
    public static void setup()
    {
        stubController = new StubServerController();
    }

    @AfterEach
    public void killServers()
    {
        stubController.reset();
    }

    @Test
    void shouldCloseChannelWhenResetFails() throws Exception
    {
        StubServer server = stubController.startStub( "reset_error.script", 9001 );
        try
        {
            URI uri = URI.create( "bolt://localhost:9001" );
            Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).withoutEncryption().build();
            ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );

            try ( Driver driver = driverFactory.newInstance( uri, AuthTokens.none(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config,
                                                             SecurityPlanImpl.insecure() ) )
            {
                try ( Session session = driver.session() )
                {
                    assertEquals( 42, session.run( "RETURN 42 AS answer" ).single().get( 0 ).asInt() );
                }

                List<Channel> channels = driverFactory.pollChannels();
                // there should be a single channel
                assertEquals( 1, channels.size() );
                // and it should be closed because it failed to RESET
                assertNull( channels.get( 0 ).closeFuture().get( 30, SECONDS ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldStreamingRecordsInBatchesRx() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_rx.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                RxSession session = driver.rxSession();
                RxResult result = session.run( "MATCH (n) RETURN n.name" );
                Flux<String> records = Flux.from( result.records() ).limitRate( 2 ).map( record -> record.get( "n.name" ).asString() );
                StepVerifier.create( records ).expectNext( "Bob", "Alice", "Tina" ).verifyComplete();
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldOnlyPullRecordsWhenNeededAsyncSession() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_buffering.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                AsyncSession session = driver.asyncSession( builder().withFetchSize( 2 ).build() );

                ArrayList<String> resultList = new ArrayList<>();

                await( session.runAsync( "MATCH (n) RETURN n.name" )
                              .thenCompose( resultCursor ->
                                                    resultCursor.forEachAsync( record -> resultList.add( record.get( 0 ).asString() ) ) ) );

                assertEquals( resultList, asList( "Bob", "Alice", "Tina", "Frank", "Daisy", "Clive" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldPullAllRecordsOnListAsyncWhenOverWatermark() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_list_async.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                AsyncSession session = driver.asyncSession( builder().withFetchSize( 10 ).build() );

                ResultCursor cursor = await( session.runAsync( "MATCH (n) RETURN n.name" ) );
                List<String> records = await( cursor.listAsync( record -> record.get( 0 ).asString() ) );

                assertEquals( records, asList( "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldDiscardIfPullNotFinished() throws Throwable
    {
        StubServer server = stubController.startStub( "read_tx_v4_discard.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
        {
            Flux<List<String>> keys = Flux.usingWhen(
                    Mono.fromSupplier( driver::rxSession ),
                    session -> session.readTransaction( tx -> tx.run( "UNWIND [1,2,3,4] AS a RETURN a" ).keys() ),
                    RxSession::close );
            StepVerifier.create( keys ).expectNext( singletonList( "a" ) ).verifyComplete();
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }
}
