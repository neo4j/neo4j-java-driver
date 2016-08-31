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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.util.ServerVersion.v3_1_0;
import static org.neo4j.driver.v1.util.ServerVersion.version;

public class SummaryIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldContainBasicMetadata() throws Throwable
    {
        // Given
        Value statementParameters = Values.parameters( "limit", 10 );
        String statementText = "UNWIND [1, 2, 3, 4] AS n RETURN n AS number LIMIT {limit}";

        // When
        StatementResult result = session.run( statementText, statementParameters );

        // Then
        assertTrue( result.hasNext() );

        // When
        ResultSummary summary = result.consume();

        // Then
        assertFalse( result.hasNext() );
        assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
        assertThat( summary.statement().text(), equalTo( statementText ) );
        assertThat( summary.statement().parameters(), equalTo( statementParameters ) );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertThat( summary, equalTo( result.consume() ) );

    }

    @Test
    public void shouldContainTimeInformation()
    {
        // Given
        ResultSummary summary = session.run( "UNWIND range(1,1000) AS n RETURN n AS number" ).consume();

        // Then
        if ( version( session.server() ).greaterThanOrEqual( v3_1_0 ) )
        {
            assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThan( 0L ) );
            assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThan( 0L ) );
        }
        else
        {
            //Not passed through by older versions of the server
            assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), equalTo( -1L ) );
            assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), equalTo( -1L ) );
        }
    }

    @Test
    public void shouldContainCorrectStatistics() throws Throwable
    {
        assertThat( session.run( "CREATE (n)" ).consume().counters().nodesCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH (n) DELETE (n)" ).consume().counters().nodesDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE ()-[:KNOWS]->()" ).consume().counters().relationshipsCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH ()-[r:KNOWS]->() DELETE r" ).consume().counters().relationshipsDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE (n:ALabel)" ).consume().counters().labelsAdded(), equalTo( 1 ) );
        assertThat( session.run( "CREATE (n {magic: 42})" ).consume().counters().propertiesSet(), equalTo( 1 ) );
        assertTrue( session.run( "CREATE (n {magic: 42})" ).consume().counters().containsUpdates() );
        assertThat( session.run( "MATCH (n:ALabel) REMOVE n:ALabel " ).consume().counters().labelsRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE INDEX ON :ALabel(prop)" ).consume().counters().indexesAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP INDEX ON :ALabel(prop)" ).consume().counters().indexesRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .consume().counters().constraintsAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .consume().counters().constraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    public void shouldContainCorrectStatementType() throws Throwable
    {
        assertThat( session.run("MATCH (n) RETURN 1").consume().statementType(), equalTo( StatementType.READ_ONLY ));
        assertThat( session.run("CREATE (n)").consume().statementType(), equalTo( StatementType.WRITE_ONLY ));
        assertThat( session.run("CREATE (n) RETURN (n)").consume().statementType(), equalTo( StatementType.READ_WRITE ));
        assertThat( session.run("CREATE INDEX ON :User(p)").consume().statementType(), equalTo( StatementType.SCHEMA_WRITE ));
    }

    @Test
    public void shouldContainCorrectPlan() throws Throwable
    {
        // When
        Plan plan = session.run( "EXPLAIN MATCH (n) RETURN 1" ).consume().plan();

        // Then
        assertThat( plan.operatorType(), notNullValue() );
        assertThat( plan.arguments().size(), greaterThan( 0 ) );
        assertThat( plan.children().size(), greaterThan( 0 ) );
    }

    @Test
    public void shouldContainProfile() throws Throwable
    {
        // When
        ResultSummary summary = session.run( "PROFILE RETURN 1" ).consume();

        // Then
        assertEquals( true, summary.hasProfile() );
        assertEquals( true, summary.hasPlan() ); // Profile is a superset of plan, so plan should be available as well if profile is available
        assertEquals( summary.plan(), summary.profile() );

        ProfiledPlan profile = summary.profile();

        assertEquals( 0, profile.dbHits() );
        assertEquals( 1, profile.records() );
    }


    @Test
    public void shouldContainNotifications() throws Throwable
    {
        // When
        ResultSummary summary = session.run( "EXPLAIN MATCH (n), (m) RETURN n, m" ).consume();

        // Then
        assertEquals( true, summary.hasPlan() );
        List<Notification> notifications = summary.notifications();
        assertNotNull( notifications );
        assertThat( notifications.size(), equalTo( 1 ) );
        assertThat( notifications.get( 0 ).toString(), containsString("CartesianProduct") );

    }
}
