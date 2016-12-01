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
package org.neo4j.driver.internal;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.hamcrest.Matcher;

import org.neo4j.driver.internal.util.MatcherFactory;
import org.neo4j.driver.v1.EventLogger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.not;
import static org.neo4j.driver.internal.util.MatcherFactory.containsAtLeast;
import static org.neo4j.driver.internal.util.MatcherFactory.count;

public final class EventHandler
{
    private final List<Event> events = new ArrayList<>();
    private final ConcurrentMap<Class<?>,List<?>> handlers = new ConcurrentHashMap<>();

    public void add( Event<?> event )
    {
        synchronized ( events )
        {
            events.add( event );
        }
        List<?> handlers = this.handlers.get( event.handlerType );
        if ( handlers != null )
        {
            for ( Object handler : handlers )
            {
                try
                {
                    dispatch( event, handler );
                }
                catch ( Exception e )
                {
                    System.err.println( "Failed to dispatch event: " + event + " to handler: " + handler );
                    e.printStackTrace( System.err );
                }
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    public <Handler> void registerHandler( Class<Handler> type, Handler handler )
    {
        List handlers = this.handlers.get( type );
        if ( handlers == null )
        {
            List<?> candidate = new CopyOnWriteArrayList<>();
            handlers = this.handlers.putIfAbsent( type, candidate );
            if ( handlers == null )
            {
                handlers = candidate;
            }
        }
        handlers.add( handler );
    }

    @SafeVarargs
    public final void assertContains( MatcherFactory<? extends Event>... matchers )
    {
        synchronized ( events )
        {
            assertThat( events, containsAtLeast( (MatcherFactory[]) matchers ) );
        }
    }

    @SafeVarargs
    public final void assertContains( Matcher<? extends Event>... matchers )
    {
        synchronized ( events )
        {
            assertThat( events, containsAtLeast( (Matcher[]) matchers ) );
        }
    }

    public final void assertCount( Matcher<? extends Event> matcher, Matcher<Integer> count )
    {
        synchronized ( events )
        {
            assertThat( events, (Matcher) count( matcher, count ) );
        }
    }

    public void assertNone( Matcher<? extends Event> matcher )
    {
        synchronized ( events )
        {
            assertThat( events, not( containsAtLeast( (Matcher) matcher ) ) );
        }
    }

    public void printEvents( PrintStream out )
    {
        printEvents( any( Event.class ), out );
    }

    public void printEvents( Matcher<? extends Event> matcher, PrintStream out )
    {
        try ( PrintWriter writer = new PrintWriter( out ) )
        {
            printEvents( matcher, writer );
        }
    }

    public void printEvents( PrintWriter out )
    {
        printEvents( any( Event.class ), out );
    }

    public void printEvents( Matcher<? extends Event> matcher, PrintWriter out )
    {
        synchronized ( events )
        {
            for ( Event event : events )
            {
                if ( matcher.matches( event ) )
                {
                    write( event, out );
                    out.println();
                }
            }
        }
    }

    public void forEach( Object handler )
    {
        synchronized ( events )
        {
            for ( Event event : events )
            {
                if ( event.handlerType.isInstance( handler ) )
                {
                    dispatch( event, handler );
                }
            }
        }
    }

    private static <Handler> void dispatch( Event<Handler> event, Object handler )
    {
        event.dispatch( event.handlerType.cast( handler ) );
    }

    static <Handler> void write( Event<Handler> event, PrintWriter out )
    {
        dispatch( event, proxy( event.handlerType, new WriteHandler( out ) ) );
    }

    private static <Handler> Handler proxy( Class<Handler> handlerType, InvocationHandler handler )
    {
        if ( handlerType.isInstance( handler ) )
        {
            return handlerType.cast( handler );
        }
        try
        {
            return handlerType.cast( proxies.get( handlerType ).newInstance( handler ) );
        }
        catch ( RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static final ClassValue<Constructor> proxies = new ClassValue<Constructor>()
    {
        @Override
        protected Constructor computeValue( Class<?> type )
        {
            Class<?> proxy = Proxy.getProxyClass( type.getClassLoader(), type );
            try
            {
                return proxy.getConstructor( InvocationHandler.class );
            }
            catch ( NoSuchMethodException e )
            {
                throw new RuntimeException( e );
            }
        }
    };

    private static class WriteHandler implements InvocationHandler, EventLogger.Sink
    {
        private final PrintWriter out;

        WriteHandler( PrintWriter out )
        {
            this.out = out;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            out.append( method.getName() ).append( '(' );
            String sep = " ";
            for ( Object arg : args )
            {
                out.append( sep );
                if ( arg == null || !arg.getClass().isArray() )
                {
                    out.append( Objects.toString( arg ) );
                }
                else if ( arg instanceof Object[] )
                {
                    out.append( Arrays.toString( (Object[]) arg ) );
                }
                else
                {
                    out.append( Arrays.class.getMethod( "toString", arg.getClass() ).invoke( null, arg ).toString() );
                }
                sep = ", ";
            }
            if ( args.length > 0 )
            {
                out.append( ' ' );
            }
            out.append( ')' );
            return null;
        }

        @Override
        public void log( String name, EventLogger.Level level, Throwable cause, String message, Object... params )
        {
            out.append( level.name() ).append( ": " );
            if ( params != null )
            {
                try
                {
                    out.format( message, params );
                }
                catch ( Exception e )
                {
                    out.format( "InvalidFormat(message=\"%s\", params=%s, failure=%s)",
                            message, Arrays.toString( params ), e );
                }
                out.println();
            }
            else
            {
                out.println( message );
            }
            if ( cause != null )
            {
                cause.printStackTrace( out );
            }
        }
    }
}
