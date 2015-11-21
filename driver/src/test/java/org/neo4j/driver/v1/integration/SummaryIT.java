/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SummaryIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldContainBasicMetadata() throws Throwable
    {
        // Given
        Map<String, Value> statementParameters = Values.parameters( "limit", 10 );
        String statementText = "UNWIND [1, 2, 3, 4] AS n RETURN n AS number LIMIT {limit}";

        // When
        Result result = session.run( statementText, statementParameters );

        // Then
        assertTrue( result.next() );

        // When
        ResultSummary summary = result.summarize();

        // Then
        assertFalse( result.next() );
        assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
        assertThat( summary.statement().text(), equalTo( statementText ) );
        assertThat( summary.statement().parameters(), equalTo( statementParameters ) );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertThat( summary, equalTo( result.summarize() ) );
    }

    @Test
    public void shouldContainCorrectStatistics() throws Throwable
    {
        assertThat( session.run( "CREATE (n)" ).summarize().updateStatistics().nodesCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH (n) DELETE (n)" ).summarize().updateStatistics().nodesDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE ()-[:KNOWS]->()" ).summarize().updateStatistics().relationshipsCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH ()-[r:KNOWS]->() DELETE r" ).summarize().updateStatistics().relationshipsDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE (n:ALabel)" ).summarize().updateStatistics().labelsAdded(), equalTo( 1 ) );
        assertThat( session.run( "CREATE (n {magic: 42})" ).summarize().updateStatistics().propertiesSet(), equalTo( 1 ) );
        assertTrue( session.run( "CREATE (n {magic: 42})" ).summarize().updateStatistics().containsUpdates() );
        assertThat( session.run( "MATCH (n:ALabel) REMOVE n:ALabel " ).summarize().updateStatistics().labelsRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE INDEX ON :ALabel(prop)" ).summarize().updateStatistics().indexesAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP INDEX ON :ALabel(prop)" ).summarize().updateStatistics().indexesRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .summarize().updateStatistics().constraintsAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .summarize().updateStatistics().constraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    public void shouldContainCorrectStatementType() throws Throwable
    {
        assertThat( session.run("MATCH (n) RETURN 1").summarize().statementType(), equalTo( StatementType.READ_ONLY ));
        assertThat( session.run("CREATE (n)").summarize().statementType(), equalTo( StatementType.WRITE_ONLY ));
        assertThat( session.run("CREATE (n) RETURN (n)").summarize().statementType(), equalTo( StatementType.READ_WRITE ));
        assertThat( session.run("CREATE INDEX ON :User(p)").summarize().statementType(), equalTo( StatementType.SCHEMA_WRITE ));
    }

    @Test
    public void shouldContainCorrectPlan() throws Throwable
    {
        // When
        Plan plan = session.run( "EXPLAIN MATCH (n) RETURN 1" ).summarize().plan();

        // Then
        assertThat( plan.operatorType(), notNullValue() );
        assertThat( plan.arguments().size(), greaterThan( 0 ) );
        assertThat( plan.children().size(), greaterThan( 0 ) );
    }

    @Test
    public void shouldContainProfile() throws Throwable
    {
        // When
        ResultSummary summary = session.run( "PROFILE RETURN 1" ).summarize();

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
        ResultSummary summary = session.run( "EXPLAIN MATCH (n), (m) RETURN n, m" ).summarize();

        // Then
        assertEquals( true, summary.hasPlan() );
        List<Notification> notifications = summary.notifications();
        assertNotNull( notifications );
        assertThat( notifications.size(), equalTo( 1 ) );

        assertThat( notifications.get( 0 ).toString(), equalTo("code=Neo.ClientNotification.Statement.CartesianProduct, title=This query builds a cartesian product between disconnected patterns., description=If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: (m)), position={offset=0, line=1, column=1}") );

    }
}
