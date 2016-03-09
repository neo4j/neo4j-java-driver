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
package org.neo4j.driver.internal.util;

import java.util.Iterator;

import org.neo4j.driver.internal.InternalPair;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    public static <V> String formatPairs( Function<V, String> printValue,
                                          int propertyCount,
                                          Iterable<Pair<String, V>> Entries )
    {
        switch ( propertyCount ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", internalPair( Entries.iterator().next() ).toString( printValue ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                Iterator<Pair<String, V>> iterator = Entries.iterator();
                builder.append( internalPair( iterator.next() ).toString( printValue ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( internalPair( iterator.next() ).toString( printValue ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> InternalPair<String, V> internalPair( Pair<String, V> property )
    {
        return (InternalPair<String, V>) property;
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
