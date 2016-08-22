/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import java.net.URI;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoggingIT
{
    @Rule
    public TestNeo4j server = new TestNeo4j();

    @Test
    public void logShouldRecordDebugAndTraceInfo() throws Exception
    {
        // Given
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );

        try( Driver driver = GraphDatabase.driver(
                Neo4jRunner.DEFAULT_URI,
                Config.build().withLogging( logging ).toConfig() ) )
        {
            // When
            when( logging.getLog( anyString() ) ).thenReturn( logger );
            when( logger.isDebugEnabled() ).thenReturn( true );
            when( logger.isTraceEnabled() ).thenReturn( true );

            try( Session session = driver.session() )
            {
                session.run( "CREATE (a {name:'Cat'})" );
            }
        }

        // Then
        verify( logger, atLeastOnce() ).debug( anyString() );
        verify( logger, atLeastOnce() ).trace( anyString() );
    }
}
