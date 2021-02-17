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

import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.SecurityException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.AuthTokens.basic;

public class FailedAuth<C extends AbstractContext> implements BlockingCommand<C>
{
    private final URI clusterUri;
    private final Config config;

    public FailedAuth( URI clusterUri, Config config )
    {
        this.clusterUri = clusterUri;
        this.config = config;
    }

    @Override
    public void execute( C context )
    {
        final Driver driver = GraphDatabase.driver( clusterUri, basic( "wrongUsername", "wrongPassword" ), config );
        SecurityException e = assertThrows( SecurityException.class, driver::verifyConnectivity );
        assertThat( e.getMessage(), containsString( "authentication failure" ) );
        driver.close();
    }
}
