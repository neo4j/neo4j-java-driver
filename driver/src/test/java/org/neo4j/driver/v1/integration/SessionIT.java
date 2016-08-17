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

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.junit.Assert.assertFalse;

public class SessionIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Test
    public void shouldKnowSessionIsClosed() throws Throwable
    {
        // Given
        try( Driver driver = GraphDatabase.driver( neo4j.address() ) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }

    @Test
    public void shouldHandleNullConfig() throws Throwable
    {
        // Given
        try( Driver driver = GraphDatabase.driver( neo4j.address(), AuthTokens.none(), null ) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullAuthToken() throws Throwable
    {
        // Given
        AuthToken token = null;
        try ( Driver driver = GraphDatabase.driver( neo4j.address(), token ) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }
}
