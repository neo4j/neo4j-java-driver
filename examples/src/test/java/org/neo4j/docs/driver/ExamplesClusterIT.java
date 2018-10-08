/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.driver.v1.net.ServerAddress;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.v1.util.Neo4jRunner.PASSWORD;
import static org.neo4j.driver.v1.util.Neo4jRunner.USER;

class ExamplesClusterIT
{
    @RegisterExtension
    static final ClusterExtension neo4j = new ClusterExtension();

    @Test
    void testShouldRunConfigCustomResolverExample() throws Exception
    {
        Cluster cluster = neo4j.getCluster();

        // Given
        try ( ConfigCustomResolverExample example = new ConfigCustomResolverExample( "bolt+routing://x.acme.com", USER, PASSWORD,
                cluster.cores().stream().map( c -> c.getBoltAddress() ).toArray( i -> new ServerAddress[i] ) ) )
        {
            // Then
            assertTrue( example.canConnect() );
        }
    }
}
