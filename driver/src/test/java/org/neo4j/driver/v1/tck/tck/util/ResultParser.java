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
package org.neo4j.driver.v1.tck.tck.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.tck.tck.util.Types.Type;
import static org.neo4j.driver.v1.tck.tck.util.Types.getTypeFromStringConstellation;

public class ResultParser
{
    private static Value parseString( String resultValue )
    {
        return new StringValue( (String) Type.String.getJavaValue( resultValue ) );
    }

    private static NodeValue parseNode( String input )
    {

        if ( input.charAt( 0 ) != '(' && input.charAt( input.length() - 1 ) != ')' )
        {
            throw new IllegalArgumentException(
                    format( "Node representation should start with ( and end with " + "). Got: {%s}",
                            input ) );
        }
        Map<String,Value> properties = getProperties( input );
        Collection<String> labels = getLabels( input );
        return new TestNodeValue( 0, labels, properties );
    }

    private static RelationshipValue parseRelationship( String input )
    {
        if ( input.charAt( 0 ) != '[' && input.charAt( input.length() - 1 ) != ']' )
        {
            throw new IllegalArgumentException(
                    format( "Relationship representation should start with [ and end with ]. Got: {%s}",
                            input ) );
        }
        Map<String,Value> properties = getProperties( input );
        Collection<String> type = getLabels( input );
        if ( type.size() > 1 )
        {
            throw new IllegalArgumentException( "Labels should only have single type. Found: " + type );
        }
        return new TestRelationshipValue( 0, type.iterator().next(), properties );
    }

    private static PathValue parsePath( String input )
    {
        input = input.substring( 1, input.length() - 1 );
        ArrayList<Entity> nodesAndRels = new ArrayList<>();
        int id = 0;

        if ( input.charAt( 0 ) != '(' )
        {
            throw new IllegalArgumentException(
                    format( "Path should start with node (. Got: [%s]", input ) );
        }
        String nstart = input.substring( 0, input.indexOf( ')' ) + 1 );
        input = input.substring( input.indexOf( ')' ) + 1 );
        TestNodeValue startNode = (TestNodeValue) parseNode( nstart );

        nodesAndRels.add( startNode );
        startNode.setId( id++ );
        TestNodeValue prev_n = startNode;
        while ( input.length() > 0 )
        {
            String n, rel;

            int split_at;
            boolean posDirection;

            if ( input.startsWith( "<-" ) )
            {
                input = input.substring( 2 );
                if ( !input.contains( "]-" ) )
                {
                    throw new IllegalArgumentException(
                            format( "Relationship in path representation should end with -]. Got: {%s}", input
                            ) );
                }
                split_at = input.indexOf( "]-" );
                rel = input.substring( 0, split_at + 1 );
                input = input.substring( split_at + 2 );
                posDirection = false;
            }
            else if ( input.startsWith( "-[" ) )
            {
                input = input.substring( 1 );
                if ( !input.contains( "]->" ) )
                {
                    throw new IllegalArgumentException(
                            format( "Relationship in path representation should end with ]->. Got: {%s}", input
                            ) );
                }
                split_at = input.indexOf( "->" );
                rel = input.substring( 0, split_at );
                input = input.substring( split_at + 2 );
                posDirection = true;
            }
            else
            {
                throw new IllegalArgumentException(
                        format( "Relationship in path should start with -[ or <-[. Got: %s", input ) );
            }
            if ( input.charAt( 0 ) != '(' )
            {
                throw new IllegalArgumentException(
                        format( "Path should contain node after relationship (. Got: %s", input ) );
            }
            split_at = input.indexOf( ')' );
            n = input.substring( 0, split_at + 1 );
            input = input.substring( split_at + 1 );

            TestNodeValue nValue = (TestNodeValue) parseNode( n );
            nValue.setId( id++ );
            if ( posDirection )
            {
                TestRelationship relValue = new TestRelationship( (TestRelationshipValue) parseRelationship( rel ),
                        prev_n, nValue );
                nodesAndRels.add( relValue );
            }
            else
            {
                TestRelationship relValue = new TestRelationship( (TestRelationshipValue) parseRelationship( rel ),
                        nValue, prev_n );
                nodesAndRels.add( relValue );
            }
            prev_n = nValue;
            nodesAndRels.add( nValue );
        }
        return new PathValue( new InternalPath( nodesAndRels ) );
    }

    private static Collection<String> getLabels( String input )
    {
        input = input.substring( 1, input.length() - 1 );
        int i1 = input.indexOf( "{" );
        if ( i1 != -1 )
        {
            int i2 = input.lastIndexOf( "}" );
            input = input.substring( 0, i1 ) + input.substring( i2 + 1 );
        }
        String[] values = input.split( ":" );
        if ( values[values.length - 1].contains( " " ) )
        {
            String s = values[values.length - 1];
            values[values.length - 1] = s.substring( 0, s.indexOf( " " ) );
        }
        values = Arrays.copyOfRange( values, 1, values.length );
        return Arrays.asList( values );
    }

    private static Map<String,Value> getProperties( String input )
    {
        Map<String,Object> result = getMapOfObjects( input, false );
        HashMap<String,Value> properties = new HashMap<>();
        for ( String key : result.keySet() )
        {
            properties.put( key, Values.value( result.get( key ) ) );
        }
        return properties;
    }

    private static PathValue pathToTestPath( Path p )
    {
        ArrayList<Entity> nodesAndRels = new ArrayList<>();
        TestNodeValue start = new TestNodeValue( p.start() );
        TestNodeValue prevNode = start;
        nodesAndRels.add( start );

        for ( Path.Segment segment : p )
        {
            TestNodeValue node = new TestNodeValue( segment.end() );
            TestRelationshipValue trv = new TestRelationshipValue( segment.relationship() );


            if ( trv.asRelationship().startNodeId() == segment.start().id() &&
                 trv.asRelationship().endNodeId() == segment.end().id() )
            {
                nodesAndRels.add( new TestRelationship( trv, prevNode, node ) );
            }
            else if ( trv.asRelationship().startNodeId() == segment.end().id() &&
                      trv.asRelationship().endNodeId() == segment.start().id() )
            {
                nodesAndRels.add( new TestRelationship( trv, node, prevNode ) );
            }
            else
            {
                throw new IllegalArgumentException( "Relationship start and stop does not match the segment " +
                                                    "nodes" );
            }
            nodesAndRels.add( node );

            prevNode = node;

        }
        return new PathValue( new InternalPath( nodesAndRels ) );
    }

    public static Map<String,Value> parseGiven( Map<String,Value> input )
    {
        Map<String,Value> converted = new HashMap<>();
        for ( String key : input.keySet() )
        {
            converted.put( key, parseGiven( input.get( key ) ) );
        }
        return converted;
    }

    public static Value parseGiven( Value input )
    {
        if ( TYPE_SYSTEM.LIST().isTypeOf( input ) )
        {
            List<Value> vals = new ArrayList<>();
            List<Value> givenVals = input.asList( ofValue() );
            for ( Value givenValue : givenVals )
            {
                vals.add( parseGiven( givenValue ) );
            }
            return new ListValue( vals.toArray( new Value[vals.size()] ) );
        }

        if ( TYPE_SYSTEM.INTEGER().isTypeOf( input ) ||
             TYPE_SYSTEM.FLOAT().isTypeOf( input ) ||
             TYPE_SYSTEM.BOOLEAN().isTypeOf( input ) ||
             TYPE_SYSTEM.STRING().isTypeOf( input ) ||
             TYPE_SYSTEM.NULL().isTypeOf( input ) )
        {
            return input;
        }
        else if ( TYPE_SYSTEM.NODE().isTypeOf( input ) )
        {
            return new TestNodeValue( input.asNode() );
        }
        else if ( TYPE_SYSTEM.RELATIONSHIP().isTypeOf( input ) )
        {
            return new TestRelationshipValue( input.asRelationship() );
        }
        else if ( TYPE_SYSTEM.PATH().isTypeOf( input ) )
        {
            return pathToTestPath( input.asPath() );
        }
        else
        {
            throw new IllegalArgumentException( format( "Type not handled for: %s", input ) );
        }
    }

    public static Object getJavaValueIntAsLong( String value )
    {
        return getJavaValue( value, false );
    }

    public static Object getJavaValueNormalInts( String value )
    {
        return getJavaValue( value, true );

    }

    private static Object getJavaValue( String value, boolean normalInts )
    {
        if ( isList( value ) )
        {
            ArrayList<Object> values = new ArrayList<>();
            for ( String val : getList( value ) )
            {
                values.add( Types.asObject( val ) );
            }
            return values;
        }
        else if ( isMap( value ) )
        {
            return getMapOfObjects( value, normalInts );
        }
        else
        {
            return Types.asObject( value );
        }
    }

    public static Value parseStringValue( String value )
    {
        if ( isList( value ) )
        {
            String[] resultValues = getList( value );
            Value[] valueArray = new Value[resultValues.length];
            for ( int j = 0; j < resultValues.length; j++ )
            {
                Type type = getTypeFromStringConstellation( resultValues[j] );
                valueArray[j] = createValue( resultValues[j], type );
            }
           return new ListValue( valueArray );
        }
        else
        {
            return createValue( value, getTypeFromStringConstellation( value ) );
        }
    }

    public static Map<String,Value> parseExpected( Collection<String> input, List<String> keys )
    {
        assertEquals( keys.size(), input.size() );
        Map<String,Value> converted = new HashMap<>();
        int i = 0;
        for ( String resultValue : input )
        {
            String key = keys.get( i );
            converted.put( key, parseStringValue( resultValue ) );
            i++;
        }
        return converted;
    }

    public static String[] getList( String resultValue )
    {
        return resultValue.substring( 1, resultValue.length() - 1 ).split( ", " );
    }

    public static Map<String,Object> getMapOfObjects( String input, boolean normalInts )
    {
        Map<String,Object> properties = new HashMap<>();
        int i1 = input.indexOf( "{" );
        if ( i1 == -1 )
        {
            return properties;
        }
        int i2 = input.lastIndexOf( "}" );
        input = input.substring( i1, i2 + 1 );
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure( DeserializationFeature.USE_LONG_FOR_INTS, !normalInts );
            properties = mapper.readValue( input, HashMap.class );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( "Not able to parse Node: " + input, e );
        }
        return properties;
    }

    public static boolean isList( String resultValue )
    {
        return resultValue.startsWith( "[" ) && resultValue.endsWith( "]" ) && resultValue.charAt( 1 ) != ':';
    }

    public static boolean isMap( String resultValue )
    {
        return resultValue.startsWith( "{" ) && resultValue.endsWith( "}" );
    }

    private static Value createValue( String resultValue, Type type )
    {
        switch ( type )
        {
        case Null:
            return NullValue.NULL;
        case Integer:
            return new IntegerValue( Long.valueOf( resultValue ) );
        case String:
            return parseString( resultValue );
        case Node:
            return parseNode( resultValue );
        case Relationship:
            return parseRelationship( resultValue );
        case Path:
            return parsePath( resultValue );
        default:
            throw new IllegalArgumentException( format( "Type not recognized: %s", type ) );
        }
    }

}
