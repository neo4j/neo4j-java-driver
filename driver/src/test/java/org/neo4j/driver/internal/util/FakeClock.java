/*
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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.driver.internal.EventHandler;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;

public class FakeClock implements Clock
{
    public interface EventSink
    {
        EventSink VOID = new Adapter();

        void sleep( long timestamp, long millis );

        void progress( long millis );

        class Adapter implements EventSink
        {
            @Override
            public void sleep( long timestamp, long millis )
            {
            }

            @Override
            public void progress( long millis )
            {
            }
        }
    }

    private final EventSink events;
    @SuppressWarnings( "unused"/*assigned through AtomicLongFieldUpdater*/ )
    private volatile long timestamp;
    private static final AtomicLongFieldUpdater<FakeClock> TIMESTAMP = newUpdater( FakeClock.class, "timestamp" );
    private PriorityBlockingQueue<WaitingThread> threads;

    public FakeClock( final EventHandler events, boolean progressOnSleep )
    {
        this( events == null ? null : new EventSink()
        {
            @Override
            public void sleep( long timestamp, long duration )
            {
                events.add( new Event.Sleep( Thread.currentThread(), timestamp, duration ) );
            }

            @Override
            public void progress( long timestamp )
            {
                events.add( new Event.Progress( Thread.currentThread(), timestamp ) );
            }
        }, progressOnSleep );
    }

    public FakeClock( EventSink events, boolean progressOnSleep )
    {
        this.events = events == null ? EventSink.VOID : events;
        this.threads = progressOnSleep ? null : new PriorityBlockingQueue<WaitingThread>();
    }

    @Override
    public long millis()
    {
        return timestamp;
    }

    @Override
    public void sleep( long millis )
    {
        if ( millis <= 0 )
        {
            return;
        }
        long target = timestamp + millis;
        events.sleep( target - millis, millis );
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
                if ( timestamp >= target )
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
        events.progress( TIMESTAMP.addAndGet( this, millis ) );
        if ( threads != null )
        {
            // wake up the threads that are sleeping awaiting the current time
            for ( WaitingThread thread; (thread = threads.peek()) != null; )
            {
                if ( thread.timestamp < timestamp )
                {
                    threads.remove( thread );
                    LockSupport.unpark( thread.thread );
                }
            }
        }
    }

    public static abstract class Event extends org.neo4j.driver.internal.Event<EventSink>
    {
        final Thread thread;

        private Event( Thread thread )
        {
            this.thread = thread;
        }

        public static Matcher<? extends Event> sleep( long duration )
        {
            return sleep( any( Thread.class ), any( Long.class ), equalTo( duration ) );
        }

        public static Matcher<? extends Event> sleep(
                final Matcher<Thread> thread,
                final Matcher<Long> timestamp,
                final Matcher<Long> duration )
        {
            return new TypeSafeMatcher<Sleep>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "Sleep Event on thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( "> at timestamp " )
                            .appendDescriptionOf( timestamp )
                            .appendText( " for duration " )
                            .appendDescriptionOf( timestamp )
                            .appendText( " (in milliseconds)" );
                }

                @Override
                protected boolean matchesSafely( Sleep event )
                {
                    return thread.matches( event.thread )
                            && timestamp.matches( event.timestamp )
                            && duration.matches( event.duration );
                }
            };
        }

        public static Matcher<? extends Event> progress( final Matcher<Thread> thread, final Matcher<Long> timestamp )
        {
            return new TypeSafeMatcher<Progress>()
            {
                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "Time progresses to timestamp " )
                            .appendDescriptionOf( timestamp )
                            .appendText( " by thread <" )
                            .appendDescriptionOf( thread )
                            .appendText( ">" );
                }

                @Override
                protected boolean matchesSafely( Progress event )
                {
                    return thread.matches( event.thread ) && timestamp.matches( event.timestamp );
                }
            };
        }

        private static class Sleep extends Event
        {
            private final long timestamp, duration;

            Sleep( Thread thread, long timestamp, long duration )
            {
                super( thread );
                this.timestamp = timestamp;
                this.duration = duration;
            }

            @Override
            public void dispatch( EventSink sink )
            {
                sink.sleep( timestamp, duration );
            }
        }

        private static class Progress extends Event
        {
            private final long timestamp;

            Progress( Thread thread, long timestamp )
            {
                super( thread );
                this.timestamp = timestamp;
            }

            @Override
            public void dispatch( EventSink sink )
            {
                sink.progress( timestamp );
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
