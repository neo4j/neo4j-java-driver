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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1.tck.tck.util;

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
import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ResultParser
{

    private static boolean multipleValuesRelationship( String input )
    {
        if ( multipleValues( input ) )
        {
            input = input.substring( 1, input.length() - 1 );
            return multipleValues( input );
        }
        else
        {
            return false;
        }
    }

    private static boolean multipleValues( String input )
    {
        if ( input.charAt( 0 ) == '[' )
        {
            if ( input.charAt( input.length() - 1 ) != ']' )
            {
                throw new IllegalArgumentException(
                        format( "Can't parse: %s. Excpecting a list of ints but did not" +
                                "find closing ']'", input ) );
            }
            else
            {
                return true;
            }
        }
        else
        {
            return false;
        }
    }

    private static String[] makeList( String input )
    {
        if ( multipleValues( input ) )
        {
            return input.substring( 1, input.length() - 1 ).split( "," );
        }
        else
        {
            return new String[]{input};
        }
    }

    private static String[] makeListRelationship( String input )
    {
        if ( multipleValuesRelationship( input ) )
        {
            return input.substring( 1, input.length() - 1 ).split( "," );
        }
        else
        {
            return new String[]{input};
        }
    }

    private static IntegerValue[] parseInt( String[] input )
    {
        if ( input.length == 0 )
        {
            return null;
        }
        IntegerValue[] intArray = new IntegerValue[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            intArray[i] = new IntegerValue( Long.valueOf( input[i] ) );
        }
        return intArray;
    }

    private static StringValue[] parseString( String[] input )
    {
        if ( input.length == 0 )
        {
            return null;
        }
        StringValue[] stringArray = new StringValue[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            stringArray[i] = new StringValue( input[i] );
        }
        return stringArray;
    }

    private static NodeValue[] parseNode( String[] input )
    {
        if ( input.length == 0 )
        {
            return null;
        }
        TestNodeValue[] nodeArray = new TestNodeValue[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            String stringNode = input[i];
            if ( stringNode.compareToIgnoreCase( "null" ) == 0 )
            {
                nodeArray[i] = null;
            }
            else
            {
                if ( stringNode.charAt( 0 ) != '(' && stringNode.charAt( stringNode.length() - 1 ) != ')' )
                {
                    throw new IllegalArgumentException(
                            format( "Node representation should start with ( and end with " + "). Got: {%s}",
                                    stringNode ) );
                }
                Map<String,Value> properties = getProperties( stringNode );
                Collection<String> labels = getLabels( stringNode );
                nodeArray[i] = new TestNodeValue( i, labels, properties );
            }
        }
        return nodeArray;
    }

    private static RelationshipValue[] parseRelationship( String[] input )
    {
        if ( input.length == 0 )
        {
            return null;
        }
        TestRelationshipValue[] relArray = new TestRelationshipValue[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            String stringNode = input[i];
            if ( stringNode.charAt( 0 ) != '[' && stringNode.charAt( stringNode.length() - 1 ) != ']' )
            {
                throw new IllegalArgumentException(
                        format( "Relationship representation should start with [ and end with ]. Got: {%s}",
                                stringNode ) );
            }
            Map<String,Value> properties = getProperties( stringNode );
            Collection<String> type = getLabels( stringNode );
            if ( type.size() > 1 )
            {
                throw new IllegalArgumentException( "Labels should only have single type. Found: " + type );
            }
            relArray[i] = new TestRelationshipValue( i, type.iterator().next(), properties );
        }
        TestRelationshipValue relValue = relArray[0];
        return relArray;
    }

    private static PathValue[] parsePath( String[] input )
    {
        if ( input.length == 0 )
        {
            return null;
        }
        PathValue[] pathArray = new PathValue[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            String path = input[i];
            ArrayList<Entity> nodesAndRels = new ArrayList<>();
            int id = 0;

            if ( path.charAt( 0 ) != '(' )
            {
                throw new IllegalArgumentException(
                        format( "Path should start with node (. Got: [%s]", path ) );
            }
            String nstart = path.substring( 0, path.indexOf( ')' ) + 1 );
            path = path.substring( path.indexOf( ')' ) + 1 );
            TestNodeValue startNode = (TestNodeValue) parseNode( new String[]{nstart} )[0];

            nodesAndRels.add( startNode );
            startNode.setId( id++ );
            TestNodeValue prev_n = startNode;
            while ( path.length() > 0 )
            {
                String n, rel;

                int split_at;
                boolean posDirection;

                if ( path.startsWith( "<-" ) )
                {
                    path = path.substring( 2 );
                    if ( !path.contains( "]-" ) )
                    {
                        throw new IllegalArgumentException(
                                format( "Relationship in path representation should end with -]. Got: {%s}", path
                                ) );
                    }
                    split_at = path.indexOf( "]-" );
                    rel = path.substring( 0, split_at + 1 );
                    path = path.substring( split_at + 2 );
                    posDirection = false;
                }
                else if ( path.startsWith( "-[" ) )
                {
                    path = path.substring( 1 );
                    if ( !path.contains( "]->" ) )
                    {
                        throw new IllegalArgumentException(
                                format( "Relationship in path representation should end with ]->. Got: {%s}", path
                                ) );
                    }
                    split_at = path.indexOf( "->" );
                    rel = path.substring( 0, split_at );
                    path = path.substring( split_at + 2 );
                    posDirection = true;
                }
                else
                {
                    throw new IllegalArgumentException(
                            format( "Relationship in path should start with -[ or <-[. Got: %s", path ) );
                }
                if ( path.charAt( 0 ) != '(' )
                {
                    throw new IllegalArgumentException(
                            format( "Path should contain node after relationship (. Got: %s", path ) );
                }
                split_at = path.indexOf( ')' );
                n = path.substring( 0, split_at + 1 );
                path = path.substring( split_at + 1 );

                TestNodeValue nValue = (TestNodeValue) parseNode( new String[]{n} )[0];
                nValue.setId( id++ );
                if ( posDirection )
                {
                    TestRelationship relValue = new TestRelationship( (TestRelationshipValue) parseRelationship( new
                            String[]{rel} )[0], prev_n, nValue );
                    nodesAndRels.add( relValue );
                }
                else
                {
                    TestRelationship relValue = new TestRelationship( (TestRelationshipValue) parseRelationship( new
                            String[]{rel} )[0], nValue, prev_n );
                    nodesAndRels.add( relValue );
                }
                prev_n = nValue;
                nodesAndRels.add( nValue );
            }
            pathArray[i] = new PathValue( new InternalPath( nodesAndRels ) );
        }
        return pathArray;
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
        Map<String,Value> properties = new HashMap<>();
        int i1 = input.indexOf( "{" );
        if ( i1 == -1 )
        {
            return properties;
        }
        int i2 = input.lastIndexOf( "}" );
        input = input.substring( i1 + 1, i2 );
        String[] keyValues = input.split( "," );
        for ( String kv : keyValues )
        {
            String[] keyValue = kv.split( ":" );
            String key = keyValue[0];
            if ( key.charAt( 0 ) == '"' )
            {
                key = key.substring( 1, key.length() - 1 );
            }
            String value = keyValue[1];
            if ( value.charAt( 0 ) != '"' )
            {
                properties.put( key, new IntegerValue( Long.valueOf( value ) ) );
            }
            else
            {
                properties.put( key, new StringValue( value.substring( 1, value.length() - 1 ) ) );
            }
        }
        return properties;
    }

    public static Map<String,String> getMapFromString( String input )
    {
        Map<String,Value> tmpMap = getProperties( input );
        Map<String,String> converted = new HashMap<>();
        for ( String key : tmpMap.keySet() )
        {
            converted.put( key, tmpMap.get( key ).asString() );
        }
        return converted;
    }

    public static Map<String,Value> getParametersFromListOfKeysAndValues( List<String> keys, List<String> values )
    {
        assertEquals( keys.size(), values.size() );
        Map<String,Value> params = new HashMap<>();
        for ( int i = 0; i < keys.size(); i++ )
        {
            String value = values.get( i );
            if ( value.charAt( 0 ) != '"' )
            {
                params.put( keys.get( i ), new IntegerValue( Long.valueOf( value ) ) );
            }
            else
            {
                params.put( keys.get( i ), new StringValue( value.substring( 1, value.length() - 1 ) ) );
            }
        }
        return params;
    }

    public static Value parseGivenPath( Value input )
    {
        if ( input instanceof ListValue )
        {
            List<Path> values = input.asList( Values.valueAsPath() );
            PathValue[] pathValues = new PathValue[values.size()];
            for ( int i = 0; i < values.size(); i++ )
            {
                pathValues[i] = pathToTestPath( values.get( i ) );
            }
            return new ListValue( pathValues );
        }
        else
        {
            return pathToTestPath( input.asPath() );
        }
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


            if ( trv.asRelationship().start().equals( segment.start().identity() ) &&
                 trv.asRelationship().end().equals( segment.end().identity() ) )
            {
                nodesAndRels.add( new TestRelationship( trv, prevNode, node ) );
            }
            else if ( trv.asRelationship().start().equals( segment.end().identity() ) &&
                      trv.asRelationship().end().equals( segment.start().identity() ) )
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

    public static Value parseGivenRelationship( Value input )
    {
        if ( input instanceof ListValue )
        {
            List<TestRelationshipValue> values = input.asList( relationshipToTest );
            return new ListValue( values.toArray( new TestRelationshipValue[values.size()] ) );
        }
        else
        {
            return new TestRelationshipValue( input.asRelationship() );
        }
    }

    public static Value parseGivenNode( Value input )
    {
        try
        {
            return new TestNodeValue( input.asNode() );
        }
        catch ( Uncoercible e )
        {
            if ( input instanceof NullValue )
            {
                return null;
            }
            else
            {
                throw e;
            }
        }
    }

    private static final Function<Value,TestRelationshipValue> relationshipToTest =
            new Function<Value,TestRelationshipValue>()
            {
                public TestRelationshipValue apply( Value val )
                {
                    return new TestRelationshipValue( val.asRelationship() );
                }
            };

    public static Map<String,Value> parseGiven( Map<String,Value> input, String type )
    {
        Map<String,Value> converted = new HashMap<>();
        for ( String key : input.keySet() )
        {
            converted.put( key, parseGiven( input.get( key ), type ) );
        }
        return converted;
    }

    public static Value parseGiven( Value input, String type )
    {
        switch ( type )
        {
        case "integer":
            return input;
        case "string":
            return input;
        case "node":
            return parseGivenNode( input );
        case "relationship":
            return parseGivenRelationship( input );
        case "path":
            return parseGivenPath( input );
        default:
            throw new IllegalArgumentException( format( "Type not recognized: %s", type ) );
        }
    }

    public static Map<String,Value> parseExpected( Collection<String> input, List<String> keys,
            Map<String,String> types )
    {
        assertEquals( keys.size(), input.size() );
        assertEquals( keys.size(), types.size() );
        Map<String,Value> converted = new HashMap<>();
        int i = 0;
        for ( String o : input )
        {
            Value[] values;
            Value value;
            String key = keys.get( i );
            String type = types.get( key );
            switch ( type )
            {
            case "integer":
                o = o.replaceAll( "\\s+", "" );
                values = parseInt( makeList( o ) );
                value = convertArrayToValue( values, multipleValues( o ) );
                break;
            case "string":
                values = parseString( makeList( o ) );
                value = convertArrayToValue( values, multipleValues( o ) );
                break;
            case "node":
                o = o.replaceAll( "\\s+", "" );
                values = parseNode( makeList( o ) );
                value = convertArrayToValue( values, multipleValues( o ) );
                break;
            case "relationship":
                o = o.replaceAll( "\\s+", "" );
                values = parseRelationship( makeListRelationship( o ) );
                value = convertArrayToValue( values, multipleValuesRelationship( o ) );
                break;
            case "path":
                o = o.replaceAll( "\\s+", "" );
                values = parsePath( makeList( o ) );
                value = convertArrayToValue( values, multipleValues( o ) );
                break;
            default:
                throw new IllegalArgumentException( format( "Type not recognized: %s", type ) );
            }
            converted.put( key, value );
            i++;
        }
        return converted;
    }

    public static Map<String,Value> parseExpected( Collection<String> input, List<String> keys, String type )
    {
        Map<String,String> mappedTypes = new HashMap<>();
        for ( String key : keys )
        {
            mappedTypes.put( key, type );
        }
        return parseExpected( input, keys, mappedTypes );
    }

    private static Value convertArrayToValue( Value[] values, boolean list )
    {
        if ( list )
        {
            return new ListValue( values );
        }
        else
        {
            return values[0];
        }
    }
}
