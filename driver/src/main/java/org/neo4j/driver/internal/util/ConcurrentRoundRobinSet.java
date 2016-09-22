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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * A set that exposes a method {@link #hop()} that cycles through the members of the set.
 * @param <T> the type of elements in the set
 */
public class ConcurrentRoundRobinSet<T> implements Set<T>
{
    private final ConcurrentSkipListSet<T> set;
    private T current;

    public ConcurrentRoundRobinSet()
    {
        set = new ConcurrentSkipListSet<>();
    }

    public ConcurrentRoundRobinSet( Comparator<T> comparator )
    {
        set = new ConcurrentSkipListSet<>( comparator );
    }

    public ConcurrentRoundRobinSet(ConcurrentRoundRobinSet<T> original)
    {
        set = new ConcurrentSkipListSet<>( original.set.comparator() );
        set.addAll( original );
    }

    public T hop()
    {
        if ( current == null )
        {
            current = set.first();
        }
        else
        {
            current = set.higher( current );
            //We've gone through all connections, start over
            if ( current == null )
            {
                current = set.first();
            }
        }

        if ( current == null )
        {
            throw new IllegalStateException( "nothing in the set" );
        }

        return current;
    }

    @Override
    public boolean add( T item )
    {
        return set.add( item );
    }

    @Override
    public boolean containsAll( Collection<?> c )
    {
        return set.containsAll( c );
    }

    @Override
    public boolean addAll( Collection<? extends T> c )
    {
        return set.addAll( c );
    }

    @Override
    public boolean retainAll( Collection<?> c )
    {
        return set.retainAll( c );
    }

    @Override
    public boolean removeAll( Collection<?> c )
    {
        return set.retainAll( c );
    }

    @Override
    public void clear()
    {
        set.clear();
    }

    @Override
    public boolean remove( Object o )
    {
        return set.remove( o );
    }

    public int size()
    {
        return set.size();
    }

    public boolean isEmpty()
    {
        return set.isEmpty();
    }

    @Override
    public boolean contains( Object o )
    {
        return set.contains( o );
    }

    @Override
    public Iterator<T> iterator()
    {
        return set.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return set.toArray();
    }

    @SuppressWarnings( "SuspiciousToArrayCall" )
    @Override
    public <T1> T1[] toArray( T1[] a )
    {
        return set.toArray( a );
    }
}
