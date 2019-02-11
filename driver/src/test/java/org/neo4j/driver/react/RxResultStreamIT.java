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
package org.neo4j.driver.react;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.ParallelizableIT;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.v1.Values.parameters;

@ParallelizableIT
class RxResultStreamIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAllowIteratingOverResultStream()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        List<Integer> seen = new ArrayList<>();
        Flux.from( res.records() ).doOnNext( r -> seen.add( r.get( "a" ).asInt() ) ).blockLast();
        assertEquals( asList( 1, 2, 3, 4 ), seen );
    }

    @Test
    void shouldAccessSummaryAfterIteratingOverResultStream()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        ResultSummary summary = Flux.from( res.records() ).then( Mono.from( res.summary() ) ).block();
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
        assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
    }

    @Test
    void shouldHaveFieldNamesInResult()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then

        String keys = Mono.from( res.keys() ).block();
        String recordKeys = Mono.from( res.records() ).map( record -> record.keys().toString() ).block();

        assertEquals( "n", keys );
        assertEquals( "[n]", recordKeys );
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record single = Mono.from( rs.records() ).block();

        // Then
        assertTrue( single.get( "m" ).isNull() );
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record single = Mono.from( rs.records() ).block();

        // Then
        assertTrue( single.get( "n" ).get( "age" ).isNull() );
    }

    @Test
    void shouldReturnNullKeyNullRecordOnEmptyResult()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult rs = session.run( "CREATE (n:Person {name:{name}})", parameters( "name", "Tom Hanks" ) );

        // Then
        assertThat( Mono.from( rs.keys() ).block(), nullValue() );
        assertThat( Mono.from( rs.records() ).block(), nullValue() );
    }

    @Test
    void shouldBeAbleToReuseSessionAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult res1 = session.run( "INVALID" );

        assertThrows( Exception.class, () -> Mono.from( res1.records() ).block() );

        // When
        RxResult res2 = session.run( "RETURN 1" );

        // Then
        Record record = Mono.from( res2.records() ).single().block();
        assertEquals( record.get("1").asLong(), 1L );
    }

    @Test
    void shouldErrorBothOnRecordAndSummaryAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "INVALID" );

        // When
        ClientException errorFromRecords = assertThrows( ClientException.class, () -> Mono.from( result.records() ).block() );

        // Then
        ClientException errorFromSummary = assertThrows( ClientException.class, () -> Mono.from( result.summary() ).block() );
        assertThat( errorFromSummary, equalTo( errorFromRecords ) ); // they shall throw the same error
    }

    @Test
    void shouldDiscardRecords()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        ResultSummary summary = Flux.from( result.records() )
                .limitRate( 1 ) // PULL_N, N=1
                .take( 1 ) // DISCARD_ALL after 1 item
                .then( Mono.from( result.summary() ) ) // I shall be able to receive summary
                .block();

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
    }

    // @Test
    // Re-enable this test once cypher execution engine support streaming
    void shouldStreamCorrectRecordsBackBeforeError()
    {
        RxSession session = neo4j.driver().rxSession();
        List<Integer> seen = new ArrayList<>();

        RxResult result = session.run( "UNWIND range(5, 0, -1) AS x RETURN x / x" );
        ClientException e = assertThrows( ClientException.class,
                () -> Flux.from( result.records() ).doOnNext( record -> seen.add( record.get( 0 ).asInt() ) ).blockLast() ); // we shall be able to stream for v4

        assertThat( e.getMessage(), containsString( "/ by zero" ) );

        // stream should manage to consume all elements except the last one, which produces an error
        assertEquals( asList( 1, 1, 1, 1, 1 ), seen );
    }

}
