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

import org.neo4j.driver.internal.InternalEntry;
import org.neo4j.driver.v1.Entry;
import org.neo4j.driver.v1.Function;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    public static <V> String formatEntries( Function<V, String> printValue,
                                            int propertyCount,
                                            Iterable<Entry<V>> Entries )
    {
        switch ( propertyCount ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", internalEntry( Entries.iterator().next() ).toString( printValue ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                Iterator<Entry<V>> iterator = Entries.iterator();
                builder.append( internalEntry( iterator.next() ).toString( printValue ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( internalEntry( iterator.next() ).toString( printValue ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> InternalEntry<V> internalEntry( Entry<V> property )
    {
        return (InternalEntry<V>) property;
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
