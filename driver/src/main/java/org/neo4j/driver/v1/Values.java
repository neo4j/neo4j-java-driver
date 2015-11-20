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
package org.neo4j.driver.v1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.value.BooleanValue;
import org.neo4j.driver.v1.internal.value.FloatValue;
import org.neo4j.driver.v1.internal.value.IdentityValue;
import org.neo4j.driver.v1.internal.value.IntegerValue;
import org.neo4j.driver.v1.internal.value.ListValue;
import org.neo4j.driver.v1.internal.value.MapValue;
import org.neo4j.driver.v1.internal.value.NodeValue;
import org.neo4j.driver.v1.internal.value.PathValue;
import org.neo4j.driver.v1.internal.value.RelationshipValue;
import org.neo4j.driver.v1.internal.value.StringValue;

/**
 * Utility for wrapping regular Java types and exposing them as {@link Value}
 * objects.
 */
public class Values
{
    public static final Value EmptyMap = value( Collections.emptyMap() );

    @SuppressWarnings("unchecked")
    public static Value value( Object value )
    {
        if ( value == null ) { return null; }
        if ( value instanceof Value ) { return (Value) value; }

        if ( value instanceof Identity ) { return new IdentityValue( (Identity) value ); }
        if ( value instanceof Node ) { return new NodeValue( (Node) value ); }
        if ( value instanceof Relationship ) { return new RelationshipValue( (Relationship) value ); }
        if ( value instanceof Path ) { return new PathValue( (Path) value ); }

        if ( value instanceof Map<?, ?> ) { return value( (Map<String,Object>) value ); }
        if ( value instanceof Collection<?> ) { return value( (List<Object>) value ); }
        if ( value instanceof Iterable<?> ) { return value( (Iterable<Object>) value ); }

        if ( value instanceof String ) { return value( (String) value ); }
        if ( value instanceof Boolean ) { return value( (boolean) value ); }
        if ( value instanceof Byte ) { return value( (byte) value ); }
        if ( value instanceof Character ) { return value( (char) value ); }
        if ( value instanceof Short ) { return value( (short) value ); }
        if ( value instanceof Integer ) { return value( (int) value ); }
        if ( value instanceof Long ) { return value( (long) value ); }
        if ( value instanceof Float ) { return value( (float) value ); }
        if ( value instanceof Double ) { return value( (double) value ); }

        if ( value instanceof String[] ) { return value( (String[]) value ); }
        if ( value instanceof boolean[] ) { return value( (boolean[]) value ); }
        if ( value instanceof char[] ) { return value( (char[]) value ); }
        if ( value instanceof short[] ) { return value( (short[]) value ); }
        if ( value instanceof int[] ) { return value( (int[]) value ); }
        if ( value instanceof long[] ) { return value( (long[]) value ); }
        if ( value instanceof float[] ) { return value( (float[]) value ); }
        if ( value instanceof double[] ) { return value( (double[]) value ); }
        if ( value instanceof Object[] ) { return value( Arrays.asList( (Object[]) value )); }

        throw new ClientException( "Unable to convert " + value.getClass().getName() + " to Neo4j Value." );
    }

    public static Value value( short[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( int[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( long[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( boolean[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( float[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( double[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( char[] val )
    {
        return new StringValue( new String( val ) );
    }

    public static Value value( String[] val )
    {
        StringValue[] values = new StringValue[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = new StringValue( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( Object[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( List<Object> val )
    {
        Value[] values = new Value[val.size()];
        for ( int i = 0; i < val.size(); i++ )
        {
            values[i] = value( val.get( i ) );
        }
        return new ListValue( values );
    }

    public static Value value( Iterable<Object> val )
    {
        List<Value> values = new ArrayList<>();
        for ( Object v : val )
        {
            values.add( value( v ) );
        }
        return new ListValue( values.toArray( new Value[values.size()] ) );
    }

    public static Value value( final long val )
    {
        return new IntegerValue( val );
    }

    public static Value value( final double val )
    {
        return new FloatValue( val );
    }

    public static Value value( final boolean val )
    {
        return new BooleanValue( val );
    }

    public static Value value( final char val )
    {
        return new StringValue( Character.toString( val ) );
    }

    public static Value value( final String val )
    {
        return new StringValue( val );
    }

    public static Value value( final Map<String,Object> val )
    {
        Map<String,Value> asValues = new HashMap<>( val.size() );
        for ( Map.Entry<String,Object> entry : val.entrySet() )
        {
            asValues.put( entry.getKey(), value( entry.getValue() ) );
        }
        return new MapValue( asValues );
    }

    public static Value[] values( final Object... vals )
    {
        Value[] values = new Value[vals.length];
        for ( int i = 0; i < vals.length; i++ )
        {
            values[i] = value( vals[i] );
        }
        return values;
    }

    /**
     * Helper function for creating a map of parameters, this can be used when you {@link
     * StatementRunner#run(String, Map) run} statements.
     * <p>
     * Allowed parameter types are java primitives and {@link String} as well as
     * {@link Collection} and {@link Map} objects containing java
     * primitives and {@link String} values.
     *
     * @param keysAndValues alternating sequence of keys and values
     * @return Map containing all parameters specified
     * @see StatementRunner#run(String, Map)
     */
    public static Map<String,Value> parameters( Object... keysAndValues )
    {
        if ( keysAndValues.length % 2 != 0 )
        {
            throw new ClientException( "Parameters function requires an even number " +
                                       "of arguments, " +
                                       "alternating key and value. Arguments were: " +
                                       Arrays.toString( keysAndValues ) + "." );
        }
        HashMap<String,Value> map = new HashMap<>( keysAndValues.length / 2 );
        for ( int i = 0; i < keysAndValues.length; i += 2 )
        {
            map.put( keysAndValues[i].toString(), value( keysAndValues[i + 1] ) );
        }
        return map;
    }

    public static Function<Value,Value> valueAsIs()
    {
        return VALUE;
    }

    public static Function<Value,String> valueToString()
    {
        return STRING;
    }

    public static Function<Value,Integer> valueToInt()
    {
        return INTEGER;
    }

    public static Function<Value,Long> valueToLong()
    {
        return LONG;
    }

    public static Function<Value,Float> valueToFloat()
    {
        return FLOAT;
    }

    public static Function<Value,Double> valueToDouble()
    {
        return DOUBLE;
    }

    public static Function<Value,Boolean> valueToBoolean()
    {
        return BOOLEAN;
    }

    public static <T> Function<Value,List<T>> valueToList( final Function<Value,T> innerMap )
    {
        return new Function<Value,List<T>>()
        {
            @Override
            public List<T> apply( Value value )
            {
                return value.javaList( innerMap );
            }
        };
    }

    private static final Function<Value,Value> VALUE = new Function<Value,Value>()
    {
        public Value apply( Value val )
        {
            return val;
        }
    };

    private static final Function<Value,String> STRING = new Function<Value,String>()
    {
        public String apply( Value val )
        {
            return val.javaString();
        }
    };

    private static final Function<Value,Integer> INTEGER = new Function<Value,Integer>()
    {
        public Integer apply( Value val )
        {
            return val.javaInteger();
        }
    };
    private static final Function<Value,Long> LONG = new Function<Value,Long>()
    {
        public Long apply( Value val )
        {
            return val.javaLong();
        }
    };
    private static final Function<Value,Float> FLOAT = new Function<Value,Float>()
    {
        public Float apply( Value val )
        {
            return val.javaFloat();
        }
    };
    private static final Function<Value,Double> DOUBLE = new Function<Value,Double>()
    {
        public Double apply( Value val )
        {
            return val.javaDouble();
        }
    };
    private static final Function<Value,Boolean> BOOLEAN = new Function<Value,Boolean>()
    {
        public Boolean apply( Value val )
        {
            return val.javaBoolean();
        }
    };

}
