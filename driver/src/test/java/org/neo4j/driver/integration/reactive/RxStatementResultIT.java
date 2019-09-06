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
package org.neo4j.driver.integration.reactive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

@EnabledOnNeo4jWith( BOLT_V4 )
@ParallelizableIT
class RxStatementResultIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAllowIteratingOverResultStream()
    {
        // When
        RxStatementResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessFullRecords( res );
    }

    @Test
    void shouldAllowIteratingOverLargeResultStream()
    {
        // When
        int size = 100000;
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult res = session.run( "UNWIND range(1, $size) AS x RETURN x", parameters( "size", size ) );

        // Then I should be able to iterate over the result
        StepVerifier.FirstStep<Integer> step = StepVerifier.create( Flux.from( res.records() ).limitRate( 100 ).map( r -> r.get( "x" ).asInt() ) );

        for ( int i = 1; i <= size; i++ )
        {
            step.expectNext( i );
        }
        step.expectComplete().verify();
    }

    @Test
    void shouldReturnKeysRecordsAndSummaryInOrder()
    {
        // When
        RxStatementResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessKeys( res );
        verifyCanAccessFullRecords( res );
        verifyCanAccessSummary( res );
    }

    @Test
    void shouldSecondVisitOfRecordReceiveEmptyRecordStream() throws Throwable
    {
        // When
        RxStatementResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessFullRecords( res );
        // Second visit shall return empty record stream
        verifyRecordsAlreadyDiscarded( res );
    }

    @Test
    void shouldReturnKeysSummaryAndDiscardRecords()
    {
        // When
        RxStatementResult res = sessionRunUnwind();

        verifyCanAccessKeys( res );
        verifyCanAccessSummary( res );
        verifyRecordsAlreadyDiscarded( res );
    }

    @Test
    void shouldAllowOnlySummary()
    {
        // When
        RxStatementResult res = sessionRunUnwind();

        verifyCanAccessSummary( res );
    }

    @Test
    void shouldAllowAccessKeysAndSummaryAfterRecord() throws Throwable
    {
        // Given
        RxStatementResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessFullRecords( res );

        // Access keys and summary after records
        verifyCanAccessKeys( res );
        verifyCanAccessSummary( res );

        // Multiple times allowed
        verifyCanAccessKeys( res );
        verifyCanAccessSummary( res );
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult rs =
                session.run( "CREATE (n:Person {name:$name}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        StepVerifier.create( Flux.from( rs.records() ).single() ).assertNext( record -> {
            // Then
            assertTrue( record.get( "m" ).isNull() );
        } ).expectComplete().verify();
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult rs =
                session.run( "CREATE (n:Person {name:$name}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        StepVerifier.create( Flux.from( rs.records() ).single() ).assertNext( record -> {
            // Then
            assertTrue( record.get( "n" ).get( "age" ).isNull() );
        } ).expectComplete().verify();
    }

    @Test
    void shouldHaveFieldNamesInResult()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        StepVerifier.create( res.keys() ).expectNext( "n" ).expectComplete().verify();
        StepVerifier.create( res.records() )
                .assertNext( record -> {
                    assertEquals( "[n]", record.keys().toString() );
                } )
                .expectComplete()
                .verify();
    }

    @Test
    void shouldReturnEmptyKeyAndRecordOnEmptyResult()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult rs = session.run( "CREATE (n:Person {name:$name})", parameters( "name", "Tom Hanks" ) );

        // Then
        StepVerifier.create( rs.keys() ).expectComplete().verify();
        StepVerifier.create( rs.records() ).expectComplete().verify();
    }

    @Test
    void shouldOnlyErrorRecordAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult result = session.run( "INVALID" );

        // When
        Flux<String> keys = Flux.from( result.keys() );
        Flux<Record> records = Flux.from( result.records() );
        Mono<ResultSummary> summaryMono = Mono.from( result.summary() );

        // Then
        StepVerifier.create( keys ).verifyComplete();

        StepVerifier.create( records ).expectErrorSatisfies( error -> {
            assertThat( error, instanceOf( ClientException.class ) );
            assertThat( error.getMessage(), containsString( "Invalid input" ) );
        } ).verify();

        StepVerifier.create( summaryMono )
                .assertNext( summary -> {
                    assertThat( summary.statement().text(), equalTo( "INVALID" ) );
                    assertNotNull( summary.server().address() );
                    assertNotNull( summary.server().version() );
                } ).verifyComplete();
    }


    @Test
    void shouldErrorOnSummaryIfNoRecord() throws Throwable
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult result = session.run( "INVALID" );

        // When
        Flux<String> keys = Flux.from( result.keys() );
        Mono<ResultSummary> summaryMono = Mono.from( result.summary() );

        // Then
        StepVerifier.create( keys ).verifyComplete();

        StepVerifier.create( summaryMono ).expectErrorSatisfies( error -> {
            assertThat( error, instanceOf( ClientException.class ) );
            assertThat( error.getMessage(), containsString( "Invalid input" ) );
        } ).verify();

        // The error stick with the summary forever
        StepVerifier.create( summaryMono ).expectErrorSatisfies( error -> {
            assertThat( error, instanceOf( ClientException.class ) );
            assertThat( error.getMessage(), containsString( "Invalid input" ) );
        } ).verify();
    }

    @Test
    void shouldDiscardRecords()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxStatementResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        StepVerifier.create( Flux.from( result.records() )
                .limitRate( 1 ) // PULL, N=1
                .take( 1 )      // DISCARD_ALL after 1 item
        )
                .assertNext( record -> assertThat( record.get( "a" ).asInt(), equalTo( 1 ) ) )
                .thenCancel()
                .verify();

        StepVerifier.create( result.summary() ) // I shall be able to receive summary
                .assertNext( summary -> {
                    // Then
                    assertThat( summary, notNullValue() );
                    assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
                } ).expectComplete().verify();
    }

    @Test
    void shouldStreamCorrectRecordsBackBeforeError()
    {
        RxSession session = neo4j.driver().rxSession();

        RxStatementResult result = session.run( "CYPHER runtime=interpreted UNWIND range(5, 0, -1) AS x RETURN x / x" );
        StepVerifier.create( Flux.from( result.records() ).map( record -> record.get( 0 ).asInt() ) )
                .expectNext( 1 )
                .expectNext( 1 )
                .expectNext( 1 )
                .expectNext( 1 )
                .expectNext( 1 )
                .expectErrorSatisfies( error -> {
                    assertThat( error.getMessage(), containsString( "/ by zero" ) );
                } )
                .verify();
    }

    private void verifyCanAccessSummary( RxStatementResult res )
    {
        StepVerifier.create( res.summary() ).assertNext( summary -> {
            assertThat( summary.statement().text(), equalTo( "UNWIND [1,2,3,4] AS a RETURN a" ) );
            assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
            assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
        } ).verifyComplete();
    }

    private void verifyRecordsAlreadyDiscarded( RxStatementResult res )
    {
        StepVerifier.create( Flux.from( res.records() ).map( r -> r.get( "a" ).asInt() ) )
                .expectComplete()
                .verify();
    }

    private void verifyCanAccessFullRecords( RxStatementResult res )
    {
        StepVerifier.create( Flux.from( res.records() ).map( r -> r.get( "a" ).asInt() ) ).expectNext( 1 ).expectNext( 2 ).expectNext( 3 ).expectNext(
                4 ).expectComplete().verify();
    }

    private void verifyCanAccessKeys( RxStatementResult res )
    {
        StepVerifier.create( res.keys() ).expectNext( "a" ).verifyComplete();
    }

    private RxStatementResult sessionRunUnwind()
    {
        RxSession session = neo4j.driver().rxSession();
        return session.run( "UNWIND [1,2,3,4] AS a RETURN a" );
    }
}
