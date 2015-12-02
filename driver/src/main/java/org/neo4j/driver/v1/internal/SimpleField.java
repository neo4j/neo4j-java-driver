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
package org.neo4j.driver.v1.internal;

import org.neo4j.driver.v1.Field;

public class SimpleField<V> implements Field<V>
{
    private final String key;
    private final V value;

    protected SimpleField( String key, V value )
    {
        this.key = key;
        this.value = value;
    }

    public String key()
    {
        return key;
    }

    public V value()
    {
        return value;
    }

    public static <V> Field<V> of( String key, V value )
    {
        return new SimpleField<>( key, value );
    }
}
