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
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.Notification;
import org.neo4j.driver.ProfiledPlan;
import org.neo4j.driver.Result;
import org.neo4j.driver.ResultSummary;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.util.TestNeo4jSession;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.StatementType.READ_ONLY;
import static org.neo4j.driver.StatementType.READ_WRITE;
import static org.neo4j.driver.StatementType.SCHEMA_WRITE;
import static org.neo4j.driver.StatementType.WRITE_ONLY;

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
        assertThat( summary.statementType(), equalTo( READ_ONLY ) );
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
        assertThat( session.run("MATCH (n) RETURN 1").summarize().statementType(), equalTo( READ_ONLY ));
        assertThat( session.run("CREATE (n)").summarize().statementType(), equalTo( WRITE_ONLY ));
        assertThat( session.run("CREATE (n) RETURN (n)").summarize().statementType(), equalTo( READ_WRITE ));
        assertThat( session.run("CREATE INDEX ON :User(p)").summarize().statementType(), equalTo( SCHEMA_WRITE ));
    }

    @Test
    public void shouldContainCorrectPlan() throws Throwable
    {
        assertThat( session.run("EXPLAIN MATCH (n) RETURN 1").summarize().plan().toString(), equalTo( "SimplePlanTreeNode{operatorType='ProduceResults', arguments={planner-impl=IDP, KeyNames=1, runtime=INTERPRETED, runtime-impl=INTERPRETED, version=CYPHER 3.0, EstimatedRows=float<0.0>, planner=COST}, identifiers=[1], children=[SimplePlanTreeNode{operatorType='Projection', arguments={LegacyExpression={  AUTOINT0}, EstimatedRows=float<0.0>}, identifiers=[1, n], children=[SimplePlanTreeNode{operatorType='AllNodesScan', arguments={EstimatedRows=float<0.0>}, identifiers=[n], children=[]}]}]}" ) );
        assertThat( session.run("EXPLAIN MATCH (n) CREATE (m) SET m += n RETURN m").summarize().plan().toString(), equalTo( "SimplePlanTreeNode{operatorType='ColumnFilter', arguments={runtime=INTERPRETED, planner-impl=RULE, runtime-impl=INTERPRETED, ColumnsLeft=keep columns m, version=CYPHER 3.0, planner=RULE}, identifiers=[m], children=[SimplePlanTreeNode{operatorType='UpdateGraph', arguments={UpdateActionName=MapPropertySet}, identifiers=[m, n], children=[SimplePlanTreeNode{operatorType='UpdateGraph', arguments={UpdateActionName=CreateNode}, identifiers=[m, n], children=[SimplePlanTreeNode{operatorType='AllNodes', arguments={}, identifiers=[n], children=[]}]}]}]}" ) );
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
