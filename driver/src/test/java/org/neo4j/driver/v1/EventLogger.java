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
package org.neo4j.driver.v1;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.driver.internal.Event;
import org.neo4j.driver.internal.EventHandler;

import static java.util.Objects.requireNonNull;

public class EventLogger implements Logger
{
    public static Logging provider( EventHandler events, Level level )
    {
        return provider( sink( requireNonNull( events, "events" ) ), level );
    }

    public static Logging provider( final Sink events, final Level level )
    {
        requireNonNull( events, "events" );
        requireNonNull( level, "level" );
        return new Logging()
        {
            @Override
            public Logger getLog( String name )
            {
                return new EventLogger( events, name, level );
            }
        };
    }

    public interface Sink
    {
        void log( String name, Level level, Throwable cause, String message, Object... params );
    }

    private final boolean debug, trace;
    private final Sink events;
    private final String name;

    public EventLogger( EventHandler events, String name, Level level )
    {
        this( sink( requireNonNull( events, "events" ) ), name, level );
    }

    public EventLogger( Sink events, String name, Level level )
    {
        this.events = requireNonNull( events, "events" );
        this.name = name;
        level = requireNonNull( level, "level" );
        this.debug = Level.DEBUG.compareTo( level ) <= 0;
        this.trace = Level.TRACE.compareTo( level ) <= 0;
    }

    private static Sink sink( final EventHandler events )
    {
        return new Sink()
        {
            @Override
            public void log( String name, Level level, Throwable cause, String message, Object... params )
            {
                events.add( new Entry( Thread.currentThread(), name, level, cause, message, params ) );
            }
        };
    }

    public enum Level
    {
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE
    }

    @Override
    public void error( String message, Throwable cause )
    {
        events.log( name, Level.ERROR, cause, message );
    }

    @Override
    public void info( String message, Object... params )
    {
        events.log( name, Level.INFO, null, message, params );
    }

    @Override
    public void warn( String message, Object... params )
    {
        events.log( name, Level.WARN, null, message, params );
    }

    @Override
    public void debug( String message, Object... params )
    {
        events.log( name, Level.DEBUG, null, message, params );
    }

    @Override
    public void trace( String message, Object... params )
    {
        events.log( name, Level.TRACE, null, message, params );
    }

    @Override
    public boolean isTraceEnabled()
    {
        return trace;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debug;
    }

    public static final class Entry extends Event<Sink>
    {
        private final Thread thread;
        private final String name;
        private final Level level;
        private final Throwable cause;
        private final String message;
        private final Object[] params;

        private Entry( Thread thread, String name, Level level, Throwable cause, String message, Object... params )
        {
            this.thread = thread;
            this.name = name;
            this.level = requireNonNull( level, "level" );
            this.cause = cause;
            this.message = message;
            this.params = params;
        }

        @Override
        public void dispatch( Sink sink )
        {
            sink.log( name, level, cause, message, params );
        }

        private String formatted()
        {
            return params == null ? message : String.format( message, params );
        }

        public static Matcher<Entry> logEntry(
                final Matcher<Thread> thread,
                final Matcher<String> name,
                final Matcher<Level> level,
                final Matcher<Throwable> cause,
                final Matcher<String> message,
                final Matcher<Object[]> params )
        {
            return new TypeSafeMatcher<Entry>()
            {
                @Override
                protected boolean matchesSafely( Entry entry )
                {
                    return level.matches( entry.level ) &&
                            thread.matches( entry.thread ) &&
                            name.matches( entry.name ) &&
                            cause.matches( entry.cause ) &&
                            message.matches( entry.message ) &&
                            params.matches( entry.params );
                }

                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "Log entry where level " )
                            .appendDescriptionOf( level )
                            .appendText( " name <" )
                            .appendDescriptionOf( name )
                            .appendText( "> cause <" )
                            .appendDescriptionOf( cause )
                            .appendText( "> message <" )
                            .appendDescriptionOf( message )
                            .appendText( "> and parameters <" )
                            .appendDescriptionOf( params )
                            .appendText( ">" );
                }
            };
        }

        public static Matcher<Entry> message( final Matcher<Level> level, final Matcher<String> message )
        {
            return new TypeSafeMatcher<Entry>()
            {
                @Override
                protected boolean matchesSafely( Entry entry )
                {
                    return level.matches( entry.level ) && message.matches( entry.formatted() );
                }

                @Override
                public void describeTo( Description description )
                {
                    description.appendText( "Log entry where level " ).appendDescriptionOf( level )
                            .appendText( " and formatted message " ).appendDescriptionOf( message );
                }
            };
        }
    }
}
