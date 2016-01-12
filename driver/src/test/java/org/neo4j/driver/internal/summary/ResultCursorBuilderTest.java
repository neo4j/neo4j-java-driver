/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.ParameterSupport;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import static org.neo4j.driver.v1.Values.value;

public class ResultCursorBuilderTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBuildHappyPathResult()
    {
        // Given
        ResultBuilder builder = createResultBuilder();
        builder.keys( new String[]{"a"} );
        builder.record( new Value[]{value( "Admin" )} );

        // When
        List<Record> result = builder.build().list();

        // Then
        assertThat( result.size(), equalTo( 1 ) );

        Record record = result.get( 0 );
        assertThat( record.value( 0 ).asString(), equalTo( "Admin" ) );
    }

    @Test
    public void shouldHandleEmptyTable()
    {
        // Given
        ResultBuilder builder = createResultBuilder();

        // When
        List<Record> result = builder.build().list();

        // Then
        assertThat( result.size(), equalTo( 0 ) );
    }

    private ResultBuilder createResultBuilder()
    {
        return new ResultBuilder( "<unknown>", ParameterSupport.NO_PARAMETERS );
    }
}
