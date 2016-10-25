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
package org.neo4j.driver.internal;

import org.junit.Test;

import org.neo4j.driver.internal.summary.SummaryBuilder;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableFormatterTest
{

    @Test
    public void basicTable() throws Exception
    {
        // GIVEN
        StatementResult result = mockResult( asList( "c1", "c2" ), "a", 42 );
        // WHEN
        String table = formatResult( result );
        // THEN
        assertThat( table, containsString( "| c1  | c2 |" ) );
        assertThat( table, containsString( "| \"a\" | 42 |" ) );
        assertThat( table, containsString( "1 row" ) );
    }

    @Test
    public void twoRows() throws Exception
    {
        // GIVEN
        StatementResult result = mockResult( asList( "c1", "c2" ), "a", 42, "b", 43 );
        // WHEN
        String table = formatResult( result );
        // THEN
        assertThat( table, containsString( "| \"a\" | 42 |" ) );
        assertThat( table, containsString( "| \"b\" | 43 |" ) );
        assertThat( table, containsString( "2 rows" ) );
    }

    @Test
    public void formatCollections() throws Exception
    {
        // GIVEN
        StatementResult result = mockResult( asList( "a", "b", "c" ), singletonMap( "a", 42 ), asList( 12, 13 ),
                singletonMap( "a", asList( 14, 15 ) ) );
        // WHEN
        String table = formatResult( result );
        // THEN
        assertThat( table, containsString( "| {a: 42} | [12, 13] | {a: [14, 15]} |" ) );
    }

    @Test
    public void formatEntities() throws Exception
    {
        // GIVEN
        Map<String,Value> properties = singletonMap( "name", Values.value( "Mark" ) );
        Map<String,Value> relProperties = singletonMap( "since", Values.value( 2016 ) );
        InternalNode node = new InternalNode( 12, asList( "Person" ), properties );
        InternalRelationship relationship = new InternalRelationship( 24, 12, 12, "TEST", relProperties );
        StatementResult result =
                mockResult( asList( "a", "b", "c" ), node, relationship, new InternalPath( node, relationship, node ) );
        // WHEN
        String table = formatResult( result );
        // THEN
        assertThat( table, containsString( "| (12:[Person]{name: Mark}) | (12)-[24:TEST{since: 2016}]->(12) |" ) );
        assertThat( table, containsString(
                "| (12:[Person]{name: Mark})(12)-[24:TEST{since: 2016}]->(12)(12:[Person]{name: Mark}) |" ) );
    }

    private String formatResult( StatementResult result )
    {
        StringWriter writer = new StringWriter();
        TableFormatter.getInstance().format( new PrintWriter( writer ), result );
        return writer.toString();
    }

    private StatementResult mockResult( List<String> cols, Object... data )
    {
        StatementResult result = mock( StatementResult.class );
        Statement statement = mock( Statement.class );
        ResultSummary summary = new SummaryBuilder( statement ).build();
        when( result.keys() ).thenReturn( cols );
        List<Record> records = new ArrayList<>();
        List<Object> input = asList( data );
        int width = cols.size();
        for ( int row = 0; row < input.size() / width; row++ )
        {
            records.add( record( cols, input.subList( row * width, (row + 1) * width ) ) );
        }
        when( result.list() ).thenReturn( records );
        when( result.consume() ).thenReturn( summary );
        return result;
    }

    private Record record( List<String> cols, List<Object> data )
    {
        assert cols.size() == data.size();
        Value[] values = new Value[data.size()];
        for ( int i = 0; i < data.size(); i++ )
        {
            values[i] = Values.value( data.get( i ) );
        }
        return new InternalRecord( cols, values );
    }

}
