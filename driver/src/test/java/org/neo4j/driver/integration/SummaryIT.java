/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.forDatabase;

@ParallelizableIT
class SummaryIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();
    private Session session;

    @BeforeEach
    void setup()
    {
        session = neo4j.driver().session();
    }

    @AfterEach
    void tearDown()
    {
        if ( session != null && session.isOpen() )
        {
            session.close();
        }
        session = null;
    }

    @Test
    void shouldContainBasicMetadata()
    {
        // Given
        Value statementParameters = Values.parameters( "limit", 10 );
        String statementText = "UNWIND [1, 2, 3, 4] AS n RETURN n AS number LIMIT $limit";

        // When
        StatementResult result = session.run( statementText, statementParameters );

        // Then
        assertTrue( result.hasNext() );

        // When
        ResultSummary summary = result.summary();

        // Then
        assertFalse( result.hasNext() );
        assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
        assertThat( summary.statement().text(), equalTo( statementText ) );
        assertThat( summary.statement().parameters(), equalTo( statementParameters ) );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertThat( summary, equalTo( result.summary() ) );

    }

    @Test
    void shouldContainTimeInformation()
    {
        // Given
        ResultSummary summary = session.run( "UNWIND range(1,1000) AS n RETURN n AS number" ).summary();

        // Then
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
    }

    @Test
    void shouldContainCorrectStatistics()
    {
        assertThat( session.run( "CREATE (n)" ).summary().counters().nodesCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH (n) DELETE (n)" ).summary().counters().nodesDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE ()-[:KNOWS]->()" ).summary().counters().relationshipsCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH ()-[r:KNOWS]->() DELETE r" ).summary().counters().relationshipsDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE (n:ALabel)" ).summary().counters().labelsAdded(), equalTo( 1 ) );
        assertThat( session.run( "CREATE (n {magic: 42})" ).summary().counters().propertiesSet(), equalTo( 1 ) );
        assertTrue( session.run( "CREATE (n {magic: 42})" ).summary().counters().containsUpdates() );
        assertThat( session.run( "MATCH (n:ALabel) REMOVE n:ALabel " ).summary().counters().labelsRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE INDEX ON :ALabel(prop)" ).summary().counters().indexesAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP INDEX ON :ALabel(prop)" ).summary().counters().indexesRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .summary().counters().constraintsAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                .summary().counters().constraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    @EnabledOnNeo4jWith( Neo4jFeature.BOLT_V4 )
    void shouldGetSystemUpdates() throws Throwable
    {
        try ( Session session = neo4j.driver().session( forDatabase( "system" ) ) )
        {
            StatementResult result = session.run( "CREATE USER foo SET PASSWORD 'bar'" );
            assertThat( result.summary().counters().containsUpdates(), equalTo( false ) );
            assertThat( result.summary().counters().containsSystemUpdates(), equalTo( true ) );

            StatementResult deleteResult = session.run( "DROP USER foo" );
            assertThat( deleteResult.summary().counters().containsUpdates(), equalTo( false ) );
            assertThat( deleteResult.summary().counters().containsSystemUpdates(), equalTo( true ) );
        }
    }

    @Test
    void shouldContainCorrectStatementType()
    {
        assertThat( session.run("MATCH (n) RETURN 1").summary().statementType(), equalTo( StatementType.READ_ONLY ));
        assertThat( session.run("CREATE (n)").summary().statementType(), equalTo( StatementType.WRITE_ONLY ));
        assertThat( session.run("CREATE (n) RETURN (n)").summary().statementType(), equalTo( StatementType.READ_WRITE ));
        assertThat( session.run("CREATE INDEX ON :User(p)").summary().statementType(), equalTo( StatementType.SCHEMA_WRITE ));
    }

    @Test
    void shouldContainCorrectPlan()
    {
        // When
        ResultSummary summary = session.run( "EXPLAIN MATCH (n) RETURN 1" ).summary();

        // Then
        assertTrue( summary.hasPlan() );

        Plan plan = summary.plan();
        assertThat( plan.operatorType(), notNullValue() );
        assertThat( plan.identifiers().size(), greaterThan( 0 ) );
        assertThat( plan.arguments().size(), greaterThan( 0 ) );
        assertThat( plan.children().size(), greaterThan( 0 ) );
    }

    @Test
    void shouldContainProfile()
    {
        // When
        ResultSummary summary = session.run( "PROFILE RETURN 1" ).summary();

        // Then
        assertTrue( summary.hasProfile() );
        assertTrue( summary.hasPlan() ); // Profile is a superset of plan, so plan should be available as well if profile is available
        assertEquals( summary.plan(), summary.profile() );

        ProfiledPlan profile = summary.profile();
        assertEquals( 0, profile.arguments().get( "dbHits" ).asInt() );
        assertEquals( 1, profile.arguments().get( "rows" ).asInt() );
    }

    @Test
    void shouldContainNotifications()
    {
        // When
        ResultSummary summary = session.run( "EXPLAIN MATCH (n:ThisLabelDoesNotExist) RETURN n" ).summary();

        // Then
        List<Notification> notifications = summary.notifications();
        assertNotNull( notifications );
        assertThat( notifications.size(), equalTo( 1 ) );
        Notification notification = notifications.get( 0 );
        assertThat( notification.code(), notNullValue() );
        assertThat( notification.title(), notNullValue() );
        assertThat( notification.description(), notNullValue() );
        assertThat( notification.severity(), notNullValue() );
        assertThat( notification.position(), notNullValue() );
    }

    @Test
    void shouldContainNoNotifications() throws Throwable
    {
        // When
        ResultSummary summary = session.run( "RETURN 1" ).summary();

        // Then
        assertThat( summary.notifications().size(), equalTo( 0 ) );
    }
}
