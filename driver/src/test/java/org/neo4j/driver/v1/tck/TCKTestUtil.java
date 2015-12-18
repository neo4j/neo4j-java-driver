/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1.tck;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.driver.v1.tck.BoltTypeSystemTestIT.session;

public class TCKTestUtil
{
    public static ResultCursor runSessionString( String input ) throws ClientException
    {
        assertNotNull( session() );
        return session.run( input );
    }

    public static ResultCursor runSessionWithParameters( String string, Map<String,Value> params )
            throws ClientException
    {
        assertNotNull( session() );
        return session.run( string, params );
    }

    public static ResultCursor runSessionWithStatement( Statement statement ) throws ClientException
    {
        assertNotNull( session() );
        return session.run( statement );
    }

    public static ResultCursor runSessionWithParameter( String statement, String key, Value value )
            throws ClientException
    {
        Map<String,Value> params = new HashMap<>( 1 );
        params.put( key, value );
        return runSessionWithParameters( statement, params );
    }

    public static Value getBoltValue( String type, String var )
    {
        Object o = asJavaType( type, var );
        if ( o == null )
        {
            return NullValue.NULL;
        }

        else
        {
            return Values.value( o );
        }
    }

    public static ArrayList<Object> asJavaArrayList( String type, String[] array )
    {
        ArrayList<Object> values = new ArrayList<>();
        for ( String str : array )
        {
            values.add( asJavaType( type, str ) );
        }
        return values;
    }

    public static Object asJavaType( String type, String var )
    {
        switch ( type )
        {
        case "Integer":
            return Long.valueOf( var );
        case "String":
            return var;
        case "Boolean":
            return Boolean.valueOf( var );
        case "Float":
            return Double.valueOf( var );
        case "Null":
        {
            return null;
        }
        default:
            throw new NoSuchElementException( format( "Unrecognised type: [%s]", type ) );
        }
    }

    public static Object boltValuetoJavaObject( Value val )
    {
        if ( val.getClass().equals( IntegerValue.class ) )
        {
            return Values.valueAsLong().apply( val );
        }
        else if ( val.getClass().equals( StringValue.class ) )
        {
            return Values.valueAsString().apply( val );
        }
        else if ( val.getClass().equals( BooleanValue.class ) || val.getClass().equals( BooleanValue.TRUE.getClass() )
                  || val.getClass().equals( BooleanValue.FALSE.getClass() ) )
        {
            return Values.valueAsBoolean().apply( val );
        }
        else if ( val.getClass().equals( FloatValue.class ) )
        {
            return Values.valueAsDouble().apply( val );
        }
        else if ( val.getClass().equals( NullValue.class ) )
        {
            return null;
        }
        else if ( val.getClass().equals( ListValue.class ) )
        {
            List<Object> object = new ArrayList<>();
            for ( Value value : Values.valueAsList( Values.valueAsIs() ).apply( val ) )
            {
                object.add( boltValuetoJavaObject( value ) );
            }
            return object;
        }
        else if ( val.getClass().equals( MapValue.class ) )
        {
            Map<String,Value> value_map = val.asMap( Values.valueAsIs() );
            Map<String,Object> object = new HashMap<>();
            for ( String key : value_map.keySet() )
            {
                object.put( key, boltValuetoJavaObject( value_map.get( key ) ) );
            }
            return object;
        }
        else
        {
            throw new NoSuchElementException( format( "Unrecognised bolt value: %s with class:%s", val, val
                    .getClass() ) );
        }
    }

    public static String getRandomString( long size )
    {
        StringBuilder stringBuilder = new StringBuilder();
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        while ( size-- > 0 )
        {
            stringBuilder.append( alphabet.charAt( random.nextInt( alphabet.length() ) ) );
        }
        return stringBuilder.toString();
    }

    public static List<Object> getListOfTypes( String type, long size )
    {
        List<Object> list = new ArrayList<>();
        while ( size-- > 0 )
        {
            list.add( getRandomValue( type ) );
        }
        return list;
    }

    public static Map<String, Object> getMapOfTypes( String type, long size )
    {
        Map<String, Object> map = new HashMap<>();
        while ( size-- > 0 )
        {
            map.put( "a" + size, getRandomValue( type ) );
        }
        return map;
    }

    public static InternalPath getPathOfEmptyNodesWithSize(long size)
    {
        List<Entity> entities = new ArrayList<>(  );
        for ( int i = 1; i < size+1; i++ )
        {
            if ( i % 2 != 0 )
            {
                entities.add( new InternalNode( i ) );
            }
            else
            {
                entities.add( new InternalRelationship( i, i-1, i+1, "type" ) );
            }
        }
        return new InternalPath( entities );
    }

    public static Object getRandomValue(String type)
    {
        switch ( type )
        {
        case "Integer":
            return getRandomLong();
        case "String":
            return getRandomString( 3 );
        case "Boolean":
            return getRandomBoolean();
        case "Float":
            return getRandomDouble();
        case "Null":
            return null;
        default:
            throw new NoSuchElementException( format( "Not supported type: %s", type ) );
        }
    }

    public static long getRandomLong()
    {
        Random random = new Random();
        return random.nextLong();
    }

    public static double getRandomDouble()
    {
        Random random = new Random();
        return random.nextDouble();
    }

    public static boolean getRandomBoolean()
    {
        Random random = new Random();
        return random.nextBoolean();
    }

    public static String boltValueAsCypherString( Value value )
    {
        return value.asLiteralString();
    }

    public static String[] getListFromString( String str )
    {
        return str.replaceAll( "\\[", "" )
                .replaceAll( "\\]", "" )
                .split( "," );
    }

    public static boolean databaseRunning()
    {
        return session() != null;
    }

    public interface CypherStatementRunner
    {
        ResultCursor runCypherStatement();

        ResultCursor result();
    }


    public static class MappedParametersRunner implements CypherStatementRunner
    {
        private String statement;
        private Map<String,Value> parameters;
        private ResultCursor result;

        public MappedParametersRunner( String st, String key, Value value )
        {
            statement = st;
            parameters = Collections.singletonMap( key, value );
        }

        public MappedParametersRunner( String st, Map<String,Value> params )
        {
            statement = st;
            parameters = params;
        }

        @Override
        public ResultCursor runCypherStatement()
        {
            result = runSessionWithParameters( statement, parameters );
            return result;
        }

        @Override
        public ResultCursor result()
        {
            return result;
        }
    }

    public static class StatementRunner implements CypherStatementRunner
    {
        private Statement statement;
        private ResultCursor result;

        public StatementRunner( Statement st )
        {
            statement = st;
        }

        @Override
        public ResultCursor runCypherStatement()
        {
            result = runSessionWithStatement( statement );
            return result;
        }

        @Override
        public ResultCursor result()
        {
            return result;
        }
    }

    public static class StringRunner implements CypherStatementRunner
    {
        private String statement;
        private ResultCursor result;

        public StringRunner( String st )
        {
            statement = st;
        }

        @Override
        public ResultCursor runCypherStatement()
        {
            result = TCKTestUtil.runSessionString( statement );
            return result;
        }

        @Override
        public ResultCursor result()
        {
            return result;
        }
    }
}
