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
package org.neo4j.driver.internal.util;

import java.util.Iterator;

import org.neo4j.driver.internal.InternalField;
import org.neo4j.driver.internal.InternalProperty;
import org.neo4j.driver.v1.Field;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Property;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    public static <V> String formatProperties( Function<V, String> printValue,
                                               int propertyCount,
                                               Iterable<Property<V>> properties )
    {
        switch ( propertyCount ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", internalProperty( properties.iterator().next() ).toString( printValue ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                Iterator<Property<V>> iterator = properties.iterator();
                builder.append( internalProperty( iterator.next() ).toString( printValue ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( internalProperty( iterator.next() ).toString( printValue ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> InternalProperty<V> internalProperty( Property<V> property )
    {
        return (InternalProperty<V>) property;
    }

    public static <V> String formatFields( Function<V, String> printValue,
                                               int propertyCount,
                                               Iterable<Field<V>> fields )
    {
        switch ( propertyCount ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", internalField( fields.iterator().next() ).toString( printValue ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                Iterator<Field<V>> iterator = fields.iterator();
                builder.append( internalField( iterator.next() ).toString( printValue ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( internalField( iterator.next() ).toString( printValue ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> InternalField<V> internalField( Field<V> property )
    {
        return (InternalField<V>) property;
    }

    public static <V> String formatElements( Function<V, String> printValue, V[] elements )
    {
        int elementCount = elements.length;
        switch ( elementCount ) {
            case 0:
                return "[]";

            case 1:
                return String.format( "[%s]", printValue.apply( elements[0] ) );

            default:
                StringBuilder builder = new StringBuilder();
                builder.append("[");
                builder.append( printValue.apply( elements[0] ) );
                for (int i = 1; i < elementCount; i++ )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( printValue.apply( elements[i] ) );
                }
                builder.append("]");
                return builder.toString();
        }
    }
}
