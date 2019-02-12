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
import reactor.test.StepVerifier;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.containsString;
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
        StepVerifier.create( Flux.from( res.records() ).map( r -> r.get( "a" ).asInt() ) )
                .expectNext( 1 )
                .expectNext( 2 )
                .expectNext( 3 )
                .expectNext( 4 )
                .expectComplete()
                .verify();
    }

    @Test
    void shouldAccessSummaryAfterIteratingOverResultStream()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        Mono<ResultSummary> summaryMono = Flux.from( res.records() ).then( Mono.from( res.summary() ) );
        StepVerifier.create( summaryMono ).assertNext( summary -> {
            assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
            assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
        } ).expectComplete().verify();
    }

    @Test
    void shouldHaveFieldNamesInResult()
    {
        // When
        RxSession session = neo4j.driver().rxSession();
        RxResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        StepVerifier.create( Mono.from( res.keys() ) ).expectNext( "n" ).expectComplete().verify();
        StepVerifier.create( Mono.from( res.records() ) )
                .assertNext( record -> {
                    assertEquals( "[n]", record.keys().toString() );
                } )
                .expectComplete()
                .verify();
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        StepVerifier.create( Mono.from( rs.records() ) ).assertNext( record -> {
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
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        StepVerifier.create( Mono.from( rs.records() ) ).assertNext( record -> {
            // Then
            assertTrue( record.get( "n" ).get( "age" ).isNull() );
        } ).expectComplete().verify();
    }

    @Test
    void shouldReturnNullKeyNullRecordOnEmptyResult()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult rs = session.run( "CREATE (n:Person {name:{name}})", parameters( "name", "Tom Hanks" ) );

        // Then
        StepVerifier.create( Mono.from( rs.keys() ) ).expectComplete().verify();
        StepVerifier.create( Mono.from( rs.records() ) ).expectComplete().verify();
    }

    @Test
    void shouldBeAbleToReuseSessionAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult res1 = session.run( "INVALID" );

        StepVerifier.create( Mono.from( res1.records() ) ).expectError( ClientException.class ).verify();

        // When
        RxResult res2 = session.run( "RETURN 1" );

        // Then
        StepVerifier.create( Mono.from( res2.records() ) ).assertNext( record -> {
            assertEquals( record.get("1").asLong(), 1L );
        } ).expectComplete().verify();
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
        StepVerifier.create( Flux.from( result.records() )
                .limitRate( 1 ) // PULL_N, N=1
                .take( 1 )      // DISCARD_ALL after 1 item
        )
                .assertNext( record -> assertThat( record.get( "a" ).asInt(), equalTo( 1 ) ) )
                .thenCancel()
                .verify();
        StepVerifier.create( Mono.from( result.summary() ) ) // I shall be able to receive summary
                .assertNext( summary -> {
                    // Then
                    assertThat( summary, notNullValue() );
                    assertThat( summary.statementType(), equalTo( StatementType.READ_ONLY ) );
                } ).expectComplete().verify();
    }

    // @Test
    // Re-enable this test once cypher execution engine support streaming
    void shouldStreamCorrectRecordsBackBeforeError()
    {
        RxSession session = neo4j.driver().rxSession();

        RxResult result = session.run( "UNWIND range(5, 0, -1) AS x RETURN x / x" );
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

}
