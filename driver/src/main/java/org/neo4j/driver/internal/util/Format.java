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
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.driver.v1.util.Function;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    public static <V> String formatPairs( Function<V, String> printValue,
                                          Map<String, V> entries )
    {
        Iterator<Entry<String,V>> iterator = entries.entrySet().iterator();
        switch ( entries.size() ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", keyValueString( iterator.next(), printValue ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                builder.append( keyValueString( iterator.next(), printValue ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( keyValueString( iterator.next(), printValue ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    private static <V> String keyValueString( Entry<String, V> entry, Function<V, String> printValue )
    {
        return String.format("%s: %s", entry.getKey(), printValue.apply( entry.getValue() ));
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
