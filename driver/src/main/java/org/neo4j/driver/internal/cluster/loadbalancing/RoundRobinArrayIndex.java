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
package org.neo4j.driver.internal.cluster.loadbalancing;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinArrayIndex
{
    private final AtomicInteger offset;

    RoundRobinArrayIndex()
    {
        this( 0 );
    }

    // only for testing
    RoundRobinArrayIndex( int initialOffset )
    {
        this.offset = new AtomicInteger( initialOffset );
    }

    public int next( int arrayLength )
    {
        if ( arrayLength == 0 )
        {
            return -1;
        }

        int nextOffset;
        while ( (nextOffset = offset.getAndIncrement()) < 0 )
        {
            // overflow, try resetting back to zero
            offset.compareAndSet( nextOffset + 1, 0 );
        }
        return nextOffset % arrayLength;
    }
}
