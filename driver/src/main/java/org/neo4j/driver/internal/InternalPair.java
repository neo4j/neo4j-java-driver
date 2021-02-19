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
package org.neo4j.driver.internal;

import java.util.Objects;

import org.neo4j.driver.util.Pair;

public class InternalPair<K, V> implements Pair<K, V>
{
    private final K key;
    private final V value;

    protected InternalPair( K key, V value )
    {
        Objects.requireNonNull( key );
        Objects.requireNonNull( value );
        this.key = key;
        this.value = value;
    }

    public K key()
    {
        return key;
    }

    public V value()
    {
        return value;
    }

    public static <K, V> Pair<K, V> of( K key, V value )
    {
        return new InternalPair<>( key, value );
    }

    @Override
    public String toString()
    {
        return String.format( "%s: %s", Objects.toString( key ), Objects.toString( value ) );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InternalPair<?, ?> that = (InternalPair<?, ?>) o;

        return key.equals( that.key ) && value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
