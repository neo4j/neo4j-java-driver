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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.ofInteger;
import static org.neo4j.driver.Values.value;

class InternalMapAccessorWithDefaultValueTest
{
    private static final String wrongKey = "wrong_key";

    @Test
    void shouldGetValueFromRecord()
    {
        Record record = createRecord();

        // Scalar Values
        assertThat( record.get( "NullValue", NullValue.NULL ), equalTo( NullValue.NULL ) );
        assertThat( record.get( wrongKey, NullValue.NULL ), equalTo( NullValue.NULL ) );

        assertThat( record.get( "BooleanValue", BooleanValue.FALSE ), equalTo( ( Value ) BooleanValue.TRUE ) );
        assertThat( record.get( wrongKey, BooleanValue.FALSE ), equalTo( ( Value ) BooleanValue.FALSE ) );

        assertThat( record.get( "StringValue", new StringValue( "" ) ),
                equalTo( ( Value ) new StringValue( "hello world" ) ) );
        assertThat( record.get( wrongKey, new StringValue( "" ) ), equalTo( ( Value ) new StringValue( "" ) ) );

        assertThat( record.get( "IntegerValue", new IntegerValue( -1 ) ), equalTo( ( Value ) new IntegerValue( 11 ) ) );
        assertThat( record.get( wrongKey, new IntegerValue( -1 ) ), equalTo( ( Value ) new IntegerValue( -1 ) ) );

        assertThat( record.get( "FloatValue", new FloatValue( 1.1 ) ), equalTo( ( Value ) new FloatValue( 2.2 ) ) );
        assertThat( record.get( wrongKey, new FloatValue( 1.1 ) ), equalTo( ( Value ) new FloatValue( 1.1 ) ) );

        // List
        assertThat( record.get( "ListValue", new ListValue() ),
                equalTo( (Value) new ListValue( new IntegerValue( 1 ), new IntegerValue( 2 ) ) ) );
        assertThat( record.get( wrongKey, new ListValue() ), equalTo( (Value) new ListValue() ) );

        // Map
        Value defaultMapValue = new MapValue( new HashMap<String,Value>() );
        Value realMapValue = new MapValue( createMap() );
        assertThat( record.get( "MapValue", defaultMapValue ), equalTo( realMapValue ) );
        assertThat( record.get( wrongKey, defaultMapValue ), equalTo( defaultMapValue ) );

        // Path
        Value defaultPathValue = new PathValue( new InternalPath(
                new InternalNode( 0L ),
                new InternalRelationship( 0L, 0L, 1L, "T" ),
                new InternalNode( 1L ) ) );
        Value realPathValue = new PathValue( createPath() );
        assertThat( record.get( "PathValue", defaultPathValue ), equalTo( realPathValue ) );
        assertThat( record.get( wrongKey, defaultPathValue ), equalTo( defaultPathValue ) );

        // Node
        Value defaultNodeValue = new NodeValue( new InternalNode( 0L ) );
        Value realNodeValue = new NodeValue( createNode() );
        assertThat( record.get( "NodeValue", defaultNodeValue ), equalTo( realNodeValue ) );
        assertThat( record.get( wrongKey, defaultNodeValue ), equalTo( defaultNodeValue ) );

        // Rel
        Value defaultRelValue = new RelationshipValue( new InternalRelationship( 0L, 0L, 1L, "T" ) );
        Value realRelValue = new RelationshipValue( createRel() );
        assertThat( record.get( "RelValue", defaultRelValue ), equalTo( realRelValue ) );
        assertThat( record.get( wrongKey, defaultRelValue ), equalTo( defaultRelValue ) );
    }

    @Test
    void shouldGetObjectFromRecord()
    {
        Record record = createRecord();

        // IntegerValue.asObject returns Long
        assertThat( record.get( "IntegerValue", (Object) 3 ), equalTo( (Object) new Long( 11 ) ) );
        assertThat( record.get( wrongKey, (Object) 3 ), equalTo( (Object) new Integer( 3 ) ) );
    }

    @Test
    void shouldGetNumberFromRecord()
    {
        Record record = createRecord();

        // IntegerValue.asNumber returns Long
        assertThat( record.get( "IntegerValue", (Number) 3 ), equalTo( (Object) new Long( 11 ) ) );
        assertThat( record.get( wrongKey, (Number) 3 ), equalTo( (Object) new Integer( 3 ) ) );
    }

    @Test
    void shouldGetEntityFromRecord()
    {
        Record record = createRecord();

        Entity defaultNodeEntity = new InternalNode( 0L );
        assertThat( record.get( "NodeValue", defaultNodeEntity ), equalTo( (Entity) createNode() ) );
        assertThat( record.get( wrongKey, defaultNodeEntity ), equalTo( defaultNodeEntity ) );

        Entity defaultRelEntity = new InternalRelationship( 0L, 0L, 1L, "T" );
        assertThat( record.get( "RelValue", defaultRelEntity ), equalTo( (Entity) createRel() ) );
        assertThat( record.get( wrongKey, defaultRelEntity ), equalTo( defaultRelEntity ) );
    }

    @Test
    void shouldGetNodeFromRecord()
    {
        Record record = createRecord();

        Node defaultNode = new InternalNode( 0L );
        assertThat( record.get( "NodeValue", defaultNode ), equalTo( createNode() ) );
        assertThat( record.get( wrongKey, defaultNode ), equalTo( defaultNode ) );
    }

    @Test
    void shouldGetRelFromRecord()
    {
        Record record = createRecord();

        Relationship defaultRel = new InternalRelationship( 0L, 0L, 1L, "T" );
        assertThat( record.get( "RelValue", defaultRel ), equalTo( createRel() ) );
        assertThat( record.get( wrongKey, defaultRel ), equalTo( defaultRel ) );
    }

    @Test
    void shouldGetPathFromRecord()
    {
        Record record = createRecord();

        Path defaultPath = new InternalPath(
                new InternalNode( 0L ),
                new InternalRelationship( 0L, 0L, 1L, "T" ),
                new InternalNode( 1L ) );
        assertThat( record.get( "PathValue", defaultPath ), equalTo( createPath() ) );
        assertThat( record.get( wrongKey, defaultPath ), equalTo( defaultPath ) );
    }

    @Test
    void shouldGetListOfObjectsFromRecord()
    {
        Record record = createRecord();

        List<Object> defaultValue = new ArrayList<>();
        // List of java objects, therefore IntegerValue will be converted to Long
        assertThat( record.get( "ListValue", defaultValue ), equalTo( asList( (Object) 1L, 2L ) ) );
        assertThat( record.get( wrongKey, defaultValue ), equalTo( defaultValue ) );
    }

    @Test
    void shouldGetListOfTFromRecord()
    {
        Record record = createRecord();
        List<Integer> defaultValue = new ArrayList<>();

        assertThat( record.get( "ListValue", defaultValue, ofInteger() ), equalTo( asList( 1, 2 ) ) );
        assertThat( record.get( wrongKey, defaultValue, ofInteger() ), equalTo( defaultValue ) );
    }

    @Test
    void shouldGetMapOfStringObjectFromRecord()
    {
        Record record = createRecord();

        Map<String, Object> expected = new HashMap<>();
        expected.put( "key1", 1L );
        expected.put( "key2", 2L );
        Map<String,Object> defaultValue = new HashMap<>();

        assertThat( record.get( "MapValue", defaultValue ), equalTo( expected ) );
        assertThat( record.get( wrongKey, defaultValue ), equalTo( defaultValue ) );
    }

    @Test
    void shouldGetMapOfStringTFromRecord()
    {
        Record record = createRecord();

        Map<String, Integer> expected = new HashMap<>();
        expected.put( "key1", 1 );
        expected.put( "key2", 2 );
        Map<String,Integer> defaultValue = new HashMap<>();

        assertThat( record.get( "MapValue", defaultValue, ofInteger() ), equalTo( expected ) );
        assertThat( record.get( wrongKey, defaultValue, ofInteger() ), equalTo( defaultValue ) );
    }

    @Test
    void shouldGetPrimitiveTypesFromRecord()
    {
        Record record = createRecord();

        // boolean
        assertThat( record.get( "BooleanValue", false ), equalTo( true ) );
        assertThat( record.get( wrongKey, false ), equalTo( false ) );

        // string
        assertThat( record.get( "StringValue", "" ), equalTo( "hello world" ) );
        assertThat( record.get( wrongKey, "" ), equalTo( "" ) );

        // int
        assertThat( record.get( "IntegerValue", 3 ), equalTo( 11 ) );
        assertThat( record.get( wrongKey, 3 ), equalTo( 3 ) );

        // long
        assertThat( record.get( "IntegerValue", 4L ), equalTo( 11L ) );
        assertThat( record.get( wrongKey, 4L ), equalTo( 4L ) );

        // float
        assertThat( record.get( "float", 1F ), equalTo( 0.1F ) );
        assertThat( record.get( wrongKey, 1F ), equalTo(  1F ) );

        // double
        assertThat( record.get( "FloatValue", 1.1 ), equalTo( 2.2 ) );
        assertThat( record.get( wrongKey, 1.1 ), equalTo(  1.1 ) );
    }

    @Test
    void shouldGetFromMap()
    {
        MapValue mapValue = new MapValue( createMap() );
        assertThat( mapValue.get( "key1", 0L ), equalTo( 1L ) );
        assertThat( mapValue.get( "key2", 0 ), equalTo( 2 ) );
        assertThat( mapValue.get( wrongKey, "" ), equalTo( "" ) );
    }

    @Test
    void shouldGetFromNode()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 43 ) );
        props.put( "k2", value( "hello world" ) );
        NodeValue nodeValue = new NodeValue( new InternalNode( 42L, Collections.singletonList( "L" ), props ) );

        assertThat( nodeValue.get( "k1", 0 ), equalTo( 43 ) );
        assertThat( nodeValue.get( "k2", "" ), equalTo( "hello world" ) );
        assertThat( nodeValue.get( wrongKey, 0L ), equalTo( 0L ) );
    }

    @Test
    void shouldGetFromRel()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 43 ) );
        props.put( "k2", value( "hello world" ) );
        RelationshipValue relValue = new RelationshipValue( new InternalRelationship( 0L, 0L, 1L, "T", props ) );

        assertThat( relValue.get( "k1", 0 ), equalTo( 43 ) );
        assertThat( relValue.get( "k2", "" ), equalTo( "hello world" ) );
        assertThat( relValue.get( wrongKey, 0L ), equalTo( 0L ) );
    }

    private Path createPath()
    {
        return new InternalPath(
                new InternalNode( 42L ),
                new InternalRelationship( 43L, 42L, 44L, "T" ),
                new InternalNode( 44L ) );
    }

    private Node createNode()
    {
        return new InternalNode( 1L );
    }

    private Relationship createRel()
    {
        return new InternalRelationship( 1L, 1L, 2L, "T" );
    }

    private Map<String, Value> createMap()
    {
        Map<String, Value> map = new HashMap<>();
        map.put( "key1", new IntegerValue( 1 ) );
        map.put( "key2", new IntegerValue( 2 ) );
        return map;
    }

    private Record createRecord()
    {
        Map<String, Value> map = createMap();
        Path path = createPath();
        Node node = createNode();
        Relationship rel = createRel();

        List<String> keys = asList(
                "NullValue",
                "BooleanValue",
                "StringValue",
                "IntegerValue",
                "FloatValue",
                "ListValue",
                "MapValue",
                "PathValue",
                "NodeValue",
                "RelValue",
                "float"
        );
        Value[] values = new Value[]{
                NullValue.NULL,
                BooleanValue.TRUE,
                new StringValue( "hello world" ),
                new IntegerValue( 11 ),
                new FloatValue( 2.2 ),
                new ListValue( new IntegerValue( 1 ), new IntegerValue( 2 ) ),
                new MapValue( map ),
                new PathValue( path ),
                new NodeValue( node ),
                new RelationshipValue( rel ),
                Values.value( 0.1F )
        };
        Record record = new InternalRecord( keys, values );
        return record;
    }

}
