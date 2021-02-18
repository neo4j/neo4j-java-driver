/*
 * Copyright (c) "Neo4j"
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

import java.util.List;

import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
class RxResultIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAllowIteratingOverResultStream()
    {
        // When
        RxResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessFullRecords( res );
    }

    @Test
    void shouldAllowIteratingOverLargeResultStream()
    {
        // When
        int size = 100000;
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "UNWIND range(1, $size) AS x RETURN x", parameters( "size", size ) );

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
        RxResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessKeys( res );
        verifyCanAccessFullRecords( res );
        verifyCanAccessSummary( res );
    }

    @Test
    void shouldSecondVisitOfRecordReceiveEmptyRecordStream() throws Throwable
    {
        // When
        RxResult res = sessionRunUnwind();

        // Then I should be able to iterate over the result
        verifyCanAccessFullRecords( res );
        // Second visit shall return empty record stream
        verifyRecordsAlreadyDiscarded( res );
    }

    @Test
    void shouldReturnKeysSummaryAndDiscardRecords()
    {
        // When
        RxResult res = sessionRunUnwind();

        verifyCanAccessKeys( res );
        verifyCanAccessSummary( res );
        verifyRecordsAlreadyDiscarded( res );
    }

    @Test
    void shouldAllowOnlySummary()
    {
        // When
        RxResult res = sessionRunUnwind();

        verifyCanAccessSummary( res );
    }

    @Test
    void shouldAllowAccessKeysAndSummaryAfterRecord() throws Throwable
    {
        // Given
        RxResult res = sessionRunUnwind();

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
        RxResult rs =
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
        RxResult rs =
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
        RxResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        StepVerifier.create( res.keys() ).expectNext( singletonList( "n" ) ).expectComplete().verify();
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
        RxResult rs = session.run( "CREATE (n:Person {name:$name})", parameters( "name", "Tom Hanks" ) );

        // Then
        StepVerifier.create( rs.keys() ).expectNext( emptyList() ).expectComplete().verify();
        StepVerifier.create( rs.records() ).expectComplete().verify();
    }

    @Test
    void shouldOnlyErrorRecordAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "INVALID" );

        // When
        Flux<List<String>> keys = Flux.from( result.keys() );
        Flux<Record> records = Flux.from( result.records() );
        Mono<ResultSummary> summaryMono = Mono.from( result.consume() );

        // Then
        StepVerifier.create( keys ).expectNext( emptyList() ).verifyComplete();

        StepVerifier.create( records ).expectErrorSatisfies( error -> {
            assertThat( error, instanceOf( ClientException.class ) );
            assertThat( error.getMessage(), containsString( "Invalid input" ) );
        } ).verify();

        StepVerifier.create( summaryMono )
                .assertNext( summary -> {
                    assertThat( summary.query().text(), equalTo( "INVALID" ) );
                    assertNotNull( summary.server().address() );
                    assertNotNull( summary.server().version() );
                } ).verifyComplete();
    }


    @Test
    void shouldErrorOnSummaryIfNoRecord() throws Throwable
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "INVALID" );

        // When
        Flux<List<String>> keys = Flux.from( result.keys() );
        Mono<ResultSummary> summaryMono = Mono.from( result.consume() );

        // Then
        StepVerifier.create( keys ).expectNext( emptyList() ).verifyComplete();

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
        RxResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        StepVerifier.create( Flux.from( result.records() )
                .limitRate( 1 ) // PULL, N=1
                .take( 1 )      // DISCARD_ALL after 1 item
        )
                .assertNext( record -> assertThat( record.get( "a" ).asInt(), equalTo( 1 ) ) )
                .thenCancel()
                .verify();

        StepVerifier.create( result.consume() ) // I shall be able to receive summary
                .assertNext( summary -> {
                    // Then
                    assertThat( summary, notNullValue() );
                    assertThat( summary.queryType(), equalTo( QueryType.READ_ONLY ) );
                } ).expectComplete().verify();
    }

    @Test
    void shouldStreamCorrectRecordsBackBeforeError()
    {
        RxSession session = neo4j.driver().rxSession();

        RxResult result = session.run( "CYPHER runtime=interpreted UNWIND range(5, 0, -1) AS x RETURN x / x" );
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

    @Test
    void shouldErrorToAccessRecordAfterSessionClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create( Flux.from( session.close() ).thenMany( result.records() ) ).expectErrorSatisfies( error -> {
            assertThat( error.getMessage(), containsString( "session is already closed" ) );
        } ).verify();
    }

    @Test
    void shouldErrorToAccessKeysAfterSessionClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create( Flux.from( session.close() ).thenMany( result.keys() ) ).expectErrorSatisfies( error -> {
            assertThat( error.getMessage(), containsString( "session is already closed" ) );
        } ).verify();
    }

    @Test
    void shouldErrorToAccessSummaryAfterSessionClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create( Flux.from( session.close() ).thenMany( result.consume() ) ).expectErrorSatisfies( error -> {
            assertThat( error.getMessage(), containsString( "session is already closed" ) );
        } ).verify();
    }

    @Test
    void shouldErrorToAccessRecordAfterTxClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create(
                Flux.from( session.beginTransaction() ).single()
                        .flatMap( tx -> Flux.from( tx.rollback() ).singleOrEmpty().thenReturn( tx ) )
                        .flatMapMany( tx -> tx.run( "UNWIND [1,2] AS a RETURN a" ).records() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Cannot run more queries" ) ) )
                .verify();
    }

    @Test
    void shouldErrorToAccessKeysAfterTxClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create(
                Flux.from( session.beginTransaction() ).single()
                        .flatMap( tx -> Flux.from( tx.rollback() ).singleOrEmpty().thenReturn( tx ) )
                        .flatMapMany( tx -> tx.run( "UNWIND [1,2] AS a RETURN a" ).keys() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Cannot run more queries" ) ) )
                .verify();
    }

    @Test
    void shouldErrorToAccessSummaryAfterTxClose()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create(
                Flux.from( session.beginTransaction() ).single()
                        .flatMap( tx -> Flux.from( tx.rollback() ).singleOrEmpty().thenReturn( tx ) )
                        .flatMapMany( tx -> tx.run( "UNWIND [1,2] AS a RETURN a" ).consume() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Cannot run more queries" ) ) )
                .verify();
    }

    @Test
    void throwErrorAfterKeys()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "UNWIND [1,2] AS a RETURN a" );

        // When
        StepVerifier.create(
                Flux.from( session.beginTransaction() ).single()
                        .flatMap( tx -> Flux.from( tx.rollback() ).singleOrEmpty().thenReturn( tx ) )
                        .flatMapMany( tx -> tx.run( "UNWIND [1,2] AS a RETURN a" ).consume() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Cannot run more queries" ) ) )
                .verify();
    }

    @Test
    void throwTheSameErrorWhenCallingConsumeMultipleTimes()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "Invalid" );

        // When
        StepVerifier.create( Flux.from( result.consume() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Invalid" ) ) )
                .verify();

        StepVerifier.create( Flux.from( result.consume() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Invalid" ) ) )
                .verify();
    }

    @Test
    void keysShouldNotReportRunError()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "Invalid" );

        // When
        StepVerifier.create( Flux.from( result.keys() ) ).expectNext( EMPTY_LIST ).verifyComplete();
        StepVerifier.create( Flux.from( result.keys() ) ).expectNext( EMPTY_LIST ).verifyComplete();
    }

    @Test
    void throwResultConsumedErrorWhenCallingRecordsMultipleTimes()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "Invalid" );

        // When
        StepVerifier.create( Flux.from( result.records() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "Invalid" ) ) )
                .verify();

        verifyRecordsAlreadyDiscarded( result );
        verifyRecordsAlreadyDiscarded( result );
    }

    private void verifyCanAccessSummary( RxResult res )
    {
        StepVerifier.create( res.consume() ).assertNext( summary -> {
            assertThat( summary.query().text(), equalTo( "UNWIND [1,2,3,4] AS a RETURN a" ) );
            assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
            assertThat( summary.queryType(), equalTo( QueryType.READ_ONLY ) );
        } ).verifyComplete();
    }

    private void verifyRecordsAlreadyDiscarded( RxResult res )
    {
        StepVerifier.create( Flux.from( res.records() ) )
                .expectErrorSatisfies( error -> assertThat( error.getMessage(), containsString( "has already been consumed" ) ) )
                .verify();
    }

    private void verifyCanAccessFullRecords( RxResult res )
    {
        StepVerifier.create( Flux.from( res.records() ).map( r -> r.get( "a" ).asInt() ) ).expectNext( 1 ).expectNext( 2 ).expectNext( 3 ).expectNext(
                4 ).expectComplete().verify();
    }

    private void verifyCanAccessKeys( RxResult res )
    {
        StepVerifier.create( res.keys() ).expectNext( singletonList( "a" ) ).verifyComplete();
    }

    private RxResult sessionRunUnwind()
    {
        RxSession session = neo4j.driver().rxSession();
        return session.run( "UNWIND [1,2,3,4] AS a RETURN a" );
    }
}
