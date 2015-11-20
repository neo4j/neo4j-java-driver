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
package org.neo4j.driver.v1.internal.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Iterables
{
    public static int count( Iterable<?> it )
    {
        if ( it instanceof Collection ) { return ((Collection) it).size(); }
        int size = 0;
        for ( Object o : it )
        {
            size++;
        }
        return size;
    }

    public static <T> List<T> toList( Iterable<T> it )
    {
        if ( it instanceof List ) { return (List<T>) it; }
        List<T> list = new ArrayList<>();
        for ( T t : it )
        {
            list.add( t );
        }
        return list;
    }
}
