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

import org.neo4j.driver.v1.Property;
import org.neo4j.driver.v1.Value;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    public static <V extends Property<Value>> String properties( int propertyCount, Iterable<V> properties )
    {
        switch (propertyCount) {
            case 0:
                return "{}";

            case 1:
                return String.format( "{%s}", properties.iterator().next() );

            default:
                StringBuilder builder = new StringBuilder();
                builder.append("{");
                Iterator<V> iterator = properties.iterator();
                builder.append( iterator.next() );
                while( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( iterator.next() );
                }
                builder.append("}");
                return builder.toString();
        }
    }
}
