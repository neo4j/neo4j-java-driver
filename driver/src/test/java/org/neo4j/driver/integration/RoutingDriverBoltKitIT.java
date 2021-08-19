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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.util.StubServer;
import org.neo4j.driver.util.StubServerController;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.util.StubServer.insecureBuilder;

/**
 * New tests should be added to testkit (https://github.com/neo4j-drivers/testkit).
 *
 * This class exists only for the following:
 * - to keep the remaining tests that are due to be migrated
 * - to keep the tests that are currently not portable
 */
@Deprecated
class RoutingDriverBoltKitIT
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

    private static String extractNameField( Record record )
    {
        return record.get( 0 ).asString();
    }

    // RX is not currently supported in testkit.

    // This does not exactly reproduce the async and blocking versions above, as we don't have any means of ignoring
    // the flux of the RETURN 1 query (not pulling the result) like we do in above, so this is "just" a test for
    // a leader going away during the execution of a flux.
    @Test
    void shouldHandleLeaderSwitchAndRetryWhenWritingInTxFunctionRX() throws IOException, InterruptedException
    {
        // Given
        StubServer server = stubController.startStub( "acquire_endpoints_twice_v4.script", 9001 );

        // START a write server that fails on the first write attempt but then succeeds on the second
        StubServer writeServer = stubController.startStub( "not_able_to_write_server_tx_func_retries_rx.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        Driver driver = GraphDatabase.driver( uri, Config.builder().withMaxTransactionRetryTime( 1, TimeUnit.MILLISECONDS ).build() );

        Flux<String> fluxOfNames = Flux.usingWhen( Mono.fromSupplier( () -> driver.rxSession( builder().withDatabase( "mydatabase" ).build() ) ),
                session -> session.writeTransaction( tx ->
                {
                    RxResult result = tx.run( "RETURN 1" );
                    return Flux.from( result.records() ).limitRate( 100 ).thenMany( tx.run( "MATCH (n) RETURN n.name" ).records() ).limitRate( 100 ).map(
                            RoutingDriverBoltKitIT::extractNameField );
                } ), RxSession::close );

        StepVerifier.create( fluxOfNames ).expectNext( "Foo", "Bar" ).verifyComplete();

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldFailInitialDiscoveryWhenConfiguredResolverThrows()
    {
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
        when( resolver.resolve( any( ServerAddress.class ) ) ).thenThrow( new RuntimeException( "Resolution failure!" ) );

        Config config = insecureBuilder().withResolver( resolver ).build();
        final Driver driver = GraphDatabase.driver( "neo4j://my.server.com:9001", config );

        RuntimeException error = assertThrows( RuntimeException.class, driver::verifyConnectivity );
        assertEquals( "Resolution failure!", error.getMessage() );
        verify( resolver ).resolve( ServerAddress.of( "my.server.com", 9001 ) );
    }
}
