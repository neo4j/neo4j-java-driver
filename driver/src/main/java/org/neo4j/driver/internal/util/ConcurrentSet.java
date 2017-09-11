/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ConcurrentSet<E> extends AbstractSet<E>
{
    private final ConcurrentMap<E,Boolean> map = new ConcurrentHashMap<>();

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean contains( Object o )
    {
        return map.containsKey( o );
    }

    @Override
    public boolean add( E o )
    {
        return map.putIfAbsent( o, Boolean.TRUE ) == null;
    }

    @Override
    public boolean remove( Object o )
    {
        return map.remove( o ) != null;
    }

    @Override
    public void clear()
    {
        map.clear();
    }

    @Override
    public Iterator<E> iterator()
    {
        return map.keySet().iterator();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }
}
