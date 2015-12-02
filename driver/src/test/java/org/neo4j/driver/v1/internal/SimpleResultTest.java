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
package org.neo4j.driver.v1.internal;


import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.v1.ImmutableRecord;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.internal.summary.ResultBuilder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.internal.util.Iterables.map;

public class SimpleResultTest
{
    @Test
    public void iterationShouldWorksAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        assertThat( result.position(), equalTo( -1 ) );
        assertTrue( result.next() ); //-1 -> 0
        assertTrue( result.first() );
        assertFalse( result.atEnd() );
        assertThat( result.record().values(), equalTo(Arrays.asList(value("v1-1"), value( "v2-1" ))));

        assertThat( result.position(), equalTo( 0 ) );
        assertTrue( result.next() ); //0 -> 1
        assertFalse( result.first() );
        assertFalse( result.atEnd() );
        assertThat( result.record().values(), equalTo(Arrays.asList(value("v1-2"), value( "v2-2" ))));

        assertThat( result.position(), equalTo( 1 ) );
        assertTrue( result.next() ); //1 -> 2

        // THEN
        assertThat( result.position(), equalTo( 2) );
        assertTrue( result.atEnd() );
        assertFalse( result.first() );
        assertThat( result.record().values(), equalTo(Arrays.asList(value("v1-3"), value( "v2-3" ))));
        assertFalse( result.next() );
    }

    @Test
    public void firstFalseOnEmptyStream()
    {
        assertFalse( createResult( 0 ).first() );
    }

    @Test
    public void firstMovesCursorOnce()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        assertThat(result.position(), equalTo( -1 ));
        assertTrue( result.first() );
        assertThat(result.position(), equalTo( 0 ));
        assertTrue( result.first() );
        assertThat(result.position(), equalTo( 0 ));
    }

    @Test
    public void countRecordsShouldGetTheCountRight()
    {
        assertThat( createResult( 3 ).countRecords(), equalTo( 3 ) );
        assertThat( createResult( 0 ).countRecords(), equalTo( 0 ) );
    }

    @Test
    public void countRecordsShouldMoveToTheEnd()
    {
        // GIVEN
        Result result = createResult( 42 );

        // WHEN
        assertThat( result.countRecords(), equalTo( 42 ) );

        // THEN
        assertTrue( result.atEnd() );
        assertThat( result.position(), equalTo( 41 ) );
    }

    @Test
    public void singleShouldWorkAsExpected()
    {
        assertFalse( createResult( 42 ).single() );
        assertFalse( createResult( 0 ).single() );
        assertTrue( createResult( 1 ).single() );
    }

    @Test
    public void skipShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 42 );

        // WHEN
        result.skip( 22 );

        // THEN
        assertThat(result.position(), equalTo( 21 ));
        assertThat(result.record().values(), equalTo( Arrays.asList( value( "v1-22" ), value( "v2-22" ) ) ));
    }

    @Test
    public void retainShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3);

        // WHEN
        List<ImmutableRecord> records = result.retain();

        // THEN
        assertTrue(result.atEnd());
        assertThat(records, hasSize( 3 ) );
    }

    private Result createResult( int numberOfRecords )
    {
        ResultBuilder builder = new ResultBuilder( "<unknown>", ParameterSupport.NO_PARAMETERS );
        builder.keys( new String[]{"k1", "k2"} );
        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            builder.record( new Value[]{value( "v1-" + i ), value( "v2-" + i )} );
        }
        return builder.build();
    }

}
