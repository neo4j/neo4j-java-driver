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
package org.neo4j.driver.v1;

import org.neo4j.driver.v1.util.Function;

/**
 * Static utility methods for retaining records
 *
 * @see StatementResult#list()
 * @since 1.0
 */
public abstract class Records
{
    public static Function<Record,Value> column( int index )
    {
        return column( index, Values.ofValue() );
    }

    public static Function<Record, Value> column( String key )
    {
        return column( key, Values.ofValue() );
    }

    public static <T> Function<Record, T> column( final int index, final Function<Value, T> mapFunction )
    {
        return new Function<Record, T>()
        {
            @Override
            public T apply( Record record )
            {
                return mapFunction.apply( record.get( index ) );
            }
        };
    }
    public static <T> Function<Record, T> column( final String key, final Function<Value, T> mapFunction )
    {
        return new Function<Record, T>()
        {
            @Override
            public T apply( Record recordAccessor )
            {
                return mapFunction.apply( recordAccessor.get( key ) );
            }
        };
    }
}
