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
package org.neo4j.driver.internal.util;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class FakeClock implements Clock
{
    private final AtomicLong timestamp = new AtomicLong();
    private final PriorityBlockingQueue<WaitingThread> threads;

    public FakeClock()
    {
        this( false );
    }

    private FakeClock( boolean progressOnSleep )
    {
        this.threads = progressOnSleep ? null : new PriorityBlockingQueue<WaitingThread>();
    }

    @Override
    public long millis()
    {
        return timestamp.get();
    }

    @Override
    public void sleep( long millis )
    {
        if ( millis <= 0 )
        {
            return;
        }
        long target = timestamp.get() + millis;
        if ( threads == null )
        {
            progress( millis );
        }
        else
        {
            // park until the target time has been reached
            WaitingThread token = new WaitingThread( Thread.currentThread(), target );
            threads.add( token );
            for ( ; ; )
            {
                if ( timestamp.get() >= target )
                {
                    threads.remove( token );
                    return;
                }
                // park with a timeout to guarantee that we make progress even if something goes wrong
                LockSupport.parkNanos( this, TimeUnit.MILLISECONDS.toNanos( millis ) );
            }
        }
    }

    public void progress( long millis )
    {
        if ( millis < 0 )
        {
            throw new IllegalArgumentException( "time can only progress forwards" );
        }
        timestamp.addAndGet( millis );
        if ( threads != null )
        {
            // wake up the threads that are sleeping awaiting the current time
            for ( WaitingThread thread; (thread = threads.peek()) != null; )
            {
                if ( thread.timestamp < timestamp.get() )
                {
                    threads.remove( thread );
                    LockSupport.unpark( thread.thread );
                }
            }
        }
    }

    private static class WaitingThread
    {
        final Thread thread;
        final long timestamp;

        private WaitingThread( Thread thread, long timestamp )
        {
            this.thread = thread;
            this.timestamp = timestamp;
        }
    }
}
