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
package org.neo4j.driver.internal.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    // formats map using ':' as key-value separator instead of default '='
    public static <V> String formatPairs( Map<String,V> entries )
    {
        Iterator<Entry<String,V>> iterator = entries.entrySet().iterator();
        switch ( entries.size() ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", keyValueString( iterator.next() ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                builder.append( keyValueString( iterator.next() ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( keyValueString( iterator.next() ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    private static <V> String keyValueString( Entry<String,V> entry )
    {
        return String.format( "%s: %s", entry.getKey(), String.valueOf( entry.getValue() ) );
    }

    /**
     * Returns the submitted value if it is not null or an empty string if it is.
     */
    public static String valueOrEmpty( String value )
    {
        return value != null ? value : "";
    }
}
