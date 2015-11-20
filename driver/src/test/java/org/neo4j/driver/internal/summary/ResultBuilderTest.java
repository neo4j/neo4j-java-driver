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
package org.neo4j.driver.internal.summary;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.ReusableResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.internal.ParameterSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.driver.v1.Values.value;

public class ResultBuilderTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBuildHappyPathResult()
    {
        // Given
        ResultBuilder builder = createResultBuilder();
        builder.fieldNames( new String[]{"a"} );
        builder.record( new Value[]{value( "Admin" )} );

        // When
        ReusableResult result = builder.build().retain();

        // Then
        assertThat( result.size(), equalTo( 1l ) );

        Record record = result.get( 0 );
        assertThat( record.get( 0 ).javaString(), equalTo( "Admin" ) );
    }

    @Test
    public void shouldHandleEmptyTable()
    {
        // Given
        ResultBuilder builder = createResultBuilder();

        // When
        ReusableResult result = builder.build().retain();

        // Then
        assertThat( result.size(), equalTo( 0l ) );
    }

    @Test
    public void shouldThrowNoSuchSomething()
    {
        // Given
        ResultBuilder builder = createResultBuilder();
        builder.fieldNames( new String[]{"a"} );
        builder.record( new Value[]{value( "Admin" )} );

        ReusableResult result = builder.build().retain();

        // Expect
        exception.expect( ClientException.class );

        // When
        result.get( 2 );
    }

    private ResultBuilder createResultBuilder()
    {
        return new ResultBuilder( "<unknown>", ParameterSupport.NO_PARAMETERS );
    }
}
