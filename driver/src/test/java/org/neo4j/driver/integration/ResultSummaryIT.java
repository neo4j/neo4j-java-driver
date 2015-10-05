/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.util.TestSession;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.StatementType.READ_ONLY;
import static org.neo4j.driver.StatementType.READ_WRITE;
import static org.neo4j.driver.StatementType.SCHEMA_WRITE;
import static org.neo4j.driver.StatementType.WRITE_ONLY;

public class ResultSummaryIT
{
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldContainStats() throws Throwable
    {
        assertThat( session.run( "CREATE (n)" ).summarize().nodesCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH (n) DELETE (n)" ).summarize().nodesDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE ()-[:KNOWS]->()" ).summarize().relationshipsCreated(), equalTo( 1 ) );
        assertThat( session.run( "MATCH ()-[r:KNOWS]->() DELETE r" ).summarize().relationshipsDeleted(), equalTo( 1 ) );

        assertThat( session.run( "CREATE (n:ALabel)" ).summarize().labelsAdded(), equalTo( 1 ) );
        assertThat( session.run( "MATCH (n:ALabel) REMOVE n:ALabel " ).summarize().labelsRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE INDEX ON :ALabel(prop)" ).summarize().indexesAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP INDEX ON :ALabel(prop)" ).summarize().indexesRemoved(), equalTo( 1 ) );

        assertThat( session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                        .summarize().constraintsAdded(), equalTo( 1 ) );
        assertThat( session.run( "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" )
                        .summarize().constraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    public void shouldContainStatementType() throws Throwable
    {
        assertThat( session.run("MATCH (n) RETURN 1").summarize().statementType(), equalTo( READ_ONLY ));
        assertThat( session.run("CREATE (n)").summarize().statementType(), equalTo( WRITE_ONLY ));
        assertThat( session.run("CREATE (n) RETURN (n)").summarize().statementType(), equalTo( READ_WRITE ));
        assertThat( session.run("CREATE INDEX ON :User(p)").summarize().statementType(), equalTo( SCHEMA_WRITE ));
    }
}
