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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.util.StubServer;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Logging.none;

class TrustedServerProductTest
{
    private static final Config config = Config.builder()
            .withoutEncryption()
            .withLogging( none() )
            .build();

    @Test
    void shouldRejectConnectionsToNonNeo4jServers() throws Exception
    {
        StubServer server = StubServer.start( "untrusted_server.script", 9001 );
        final Driver driver = GraphDatabase.driver( "bolt://127.0.0.1:9001", config );
        assertThrows( UntrustedServerException.class, driver::verifyConnectivity );
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

}
