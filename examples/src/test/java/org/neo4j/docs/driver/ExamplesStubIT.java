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
package org.neo4j.docs.driver;

import org.junit.jupiter.api.Test;

import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.util.StubServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExamplesStubIT
{
    @Test
    void testShouldRunConfigCustomResolverExample() throws Exception
    {
        StubServer server1 = StubServer.start( "get_routing_table_only.script", 9001 );
        StubServer server2 = StubServer.start( "return_1.script", 9002 );

        // Given
        try ( ConfigCustomResolverExample example = new ConfigCustomResolverExample( "neo4j://x.example.com", ServerAddress.of( "localhost", 9001 ) ) )
        {
            // Then
            assertTrue( example.canConnect() );
        }
        finally
        {
            assertEquals( 0, server1.exitStatus() );
            assertEquals( 0, server2.exitStatus() );
        }
    }
}
