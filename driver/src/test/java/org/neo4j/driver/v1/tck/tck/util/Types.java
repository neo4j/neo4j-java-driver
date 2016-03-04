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


import java.util.ArrayList;
import java.util.Random;

import static java.lang.String.format;

public class Types
{
    public static Type getType( String stringType )
    {
        for ( Type type : Type.values() )
        {
            if ( type.toString().compareToIgnoreCase( stringType ) == 0 )
            {
                return type;
            }
        }
        throw new IllegalArgumentException( format( "There is no type: %s", stringType ) );
    }

    public static Object asObject( String object )
    {
        return getTypeFromStringConstellation( object ).getJavaValue( object );
    }

    public static Type getTypeFromStringConstellation( String object )
    {
        if ( object.length() == 0 )
        {
            throw new IllegalArgumentException( "Cannot find matching type for expression: " + object );
        }
        if ( object.startsWith( "[:" ) && object.endsWith( "]" ) )
        {
            return Type.Relationship;
        }
        if ( object.startsWith( "(" ) && object.endsWith( ")" ) )
        {
            return Type.Node;
        }
        if ( object.startsWith( "<(" ) && object.endsWith( ")>" ) )
        {
            return Type.Path;
        }
        if ( object.trim().equals( "null" ) )
        {
            return Type.Null;
        }
        if ( object.trim().equals( "true" ) || object.trim().equals( "false" ) )
        {
            return Type.Boolean;
        }
        if ( object.charAt( 0 ) == '"' && object.charAt( object.length() - 1 ) == '"' )
        {
            return Type.String;
        }
        if ( object.matches( "-?[0-9]+" ) )
        {
            return Type.Integer;
        }
        try
        {
            Double.parseDouble( object );
            return Type.Float;
        }
        catch ( Exception ignore ) {}

        throw new IllegalArgumentException( "Cannot find matching type for expression: " + object );
    }

    public enum Type implements TypeLayout
    {
        Integer
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        return Long.valueOf( val );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        ArrayList<Object> values = new ArrayList<>();
                        for ( String str : array )
                        {
                            values.add( getJavaValue( str ) );
                        }
                        return values;
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        Random random = new Random();
                        return random.nextLong();
                    }
                },
        Float
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        return Double.valueOf( val );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        ArrayList<Object> values = new ArrayList<>();
                        for ( String str : array )
                        {
                            values.add( getJavaValue( str ) );
                        }
                        return values;
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        Random random = new Random();
                        return random.nextDouble();
                    }
                },
        Boolean
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        return java.lang.Boolean.valueOf( val );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        ArrayList<Object> values = new ArrayList<>();
                        for ( String str : array )
                        {
                            values.add( getJavaValue( str ) );
                        }
                        return values;
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        Random random = new Random();
                        return random.nextBoolean();
                    }
                },
        String
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        return val.substring( 1, val.length() - 1 );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        ArrayList<Object> values = new ArrayList<>();
                        for ( String str : array )
                        {
                            values.add( getJavaValue( str ) );
                        }
                        return values;
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        int size = 4;
                        StringBuilder stringBuilder = new StringBuilder();
                        String alphabet = "abcdefghijklmnopqrstuvwxyz";
                        Random random = new Random();
                        while ( size-- > 0 )
                        {
                            stringBuilder.append( alphabet.charAt( random.nextInt( alphabet.length() ) ) );
                        }
                        return stringBuilder.toString();
                    }
                },
        Null
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        return null;
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        ArrayList<Object> values = new ArrayList<>();
                        for ( String str : array )
                        {
                            values.add( getJavaValue( str ) );
                        }
                        return values;
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        return null;
                    }
                },
        Node
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Node" );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Node" );
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        throw new IllegalArgumentException( "Not implemented" );
                    }
                },
        Relationship
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Relationship" );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Relationship" );
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        throw new IllegalArgumentException( "Not implemented" );
                    }
                },
        Path
                {
                    @Override
                    public Object getJavaValue( String val )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Path" );
                    }

                    @Override
                    public ArrayList<Object> getJavaArrayList( String[] array )
                    {
                        throw new IllegalArgumentException( "There is no native java representation of Path" );
                    }

                    @Override
                    public Object getRandomValue()
                    {
                        throw new IllegalArgumentException( "Not implemented" );
                    }
                }
    }
}
