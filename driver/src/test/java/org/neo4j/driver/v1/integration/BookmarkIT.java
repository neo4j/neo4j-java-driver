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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.ServerVersion;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import static org.neo4j.driver.v1.util.ServerVersion.v3_1_0;

public class BookmarkIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Before
    public void assumeBookmarkSupport()
    {
        System.out.println( session.server() );
        assumeTrue( ServerVersion.version( session.server() ).greaterThanOrEqual( v3_1_0 ) );
    }

    @Test
    public void shouldReceiveBookmarkOnSuccessfulCommit() throws Throwable
    {
        // Given
        assertNull( session.lastBookmark() );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.success();
        }

        // Then
        assertNotNull( session.lastBookmark() );
        assertThat( session.lastBookmark(), startsWith( "neo4j:bookmark:v1:tx" ) );
    }
}
