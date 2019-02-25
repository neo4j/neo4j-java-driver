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
package org.neo4j.driver.v1.integration.reactive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.ParallelizableIT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

@EnabledOnNeo4jWith( BOLT_V4 )
@ParallelizableIT
class RxSessionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAllowSessionRun()
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
    void shouldBeAbleToReuseSessionAfterFailure()
    {
        // Given
        RxSession session = neo4j.driver().rxSession();
        RxResult res1 = session.run( "INVALID" );

        StepVerifier.create( res1.records() ).expectError( ClientException.class ).verify();

        // When
        RxResult res2 = session.run( "RETURN 1" );

        // Then
        StepVerifier.create( res2.records() ).assertNext( record -> {
            assertEquals( record.get("1").asLong(), 1L );
        } ).expectComplete().verify();
    }
}
