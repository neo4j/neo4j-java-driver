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
package org.neo4j.driver.internal;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekingIterator<T> implements Iterator<T>
{
    private Iterator<T> iterator;
    private T cached;

    public PeekingIterator( Iterator<T> iterator )
    {
        this.iterator = iterator.hasNext() ? iterator : null;

    }

    public T peek()
    {
        return cacheNext() ? cached : null;
    }

    public boolean hasNext()
    {
        return cacheNext();
    }

    public T next()
    {
        if ( cacheNext() )
        {
            T result = cached;
            cached = null;
            return result;
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private boolean cacheNext()
    {
        if ( cached == null  )
        {
            if ( iterator == null )
            {
                return false;
            }
            else
            {
                cached = iterator.next();
                if (! iterator.hasNext()) {
                    this.iterator = null;
                }
                return true;
            }
        }
        else
        {
            return true;
        }
    }

    public void discard()
    {
        this.cached = null;
        this.iterator = null;
    }
}
