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
package org.neo4j.driver.internal.netty;

import java.util.concurrent.atomic.AtomicInteger;

public class NettyConnectionState
{
    private final AtomicInteger usageCounter = new AtomicInteger();

    public boolean markInUse()
    {
        int current;
        do
        {
            current = usageCounter.get();
            if ( current == -1 )
            {
                return false;
            }
        }
        while ( !usageCounter.compareAndSet( current, current + 1 ) );
        return true;
    }

    public boolean release()
    {
        int current;
        int next;
        do
        {
            current = usageCounter.get();
            if ( current == -1 )
            {
                return false;
            }
            next = current - 1;
        }
        while ( !usageCounter.compareAndSet( current, next ) );
        return next == -1;
    }

    public boolean forceRelease()
    {
        int previous = usageCounter.getAndSet( -1 );
        if ( previous == -1 )
        {
            return false;
        }
        else
        {
            return true;
        }
    }
}
