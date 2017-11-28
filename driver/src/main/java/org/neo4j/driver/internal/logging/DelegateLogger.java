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
package org.neo4j.driver.internal.logging;

import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;

import org.neo4j.driver.v1.Logger;

import static java.lang.String.format;


public class DelegateLogger implements InternalLogger
{
    private Logger log;
    public DelegateLogger( Logger log )
    {
        this.log = log;
    }

    @Override
    public String name()
    {
        return null;
    }

    @Override
    public boolean isTraceEnabled()
    {
        return log.isTraceEnabled();
    }

    @Override
    public void trace( String msg )
    {
        log.trace( msg );
    }

    @Override
    public void trace( String format, Object arg )
    {
        log.trace( format, arg );
    }

    @Override
    public void trace( String format, Object argA, Object argB )
    {
        log.trace( format, argA, argB );
    }

    @Override
    public void trace( String format, Object... arguments )
    {
        log.trace( format, arguments );
    }

    @Override
    public void trace( String msg, Throwable t )
    {
        log.trace( "%s%n%s", msg, t );
    }

    @Override
    public void trace( Throwable t )
    {
        trace( t.getMessage(), t );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public void debug( String msg )
    {
        log.debug( msg );
    }

    @Override
    public void debug( String format, Object arg )
    {
        log.debug( format, arg );
    }

    @Override
    public void debug( String format, Object argA, Object argB )
    {
        log.debug( format, argA, argB );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        log.debug( format, arguments );
    }

    @Override
    public void debug( String msg, Throwable t )
    {
        log.debug( "%s%n%s", msg, t );
    }

    @Override
    public void debug( Throwable t )
    {
        debug( t.getMessage(), t );
    }

    @Override
    public boolean isInfoEnabled()
    {
        return true;
    }

    @Override
    public void info( String msg )
    {
        log.info( msg );
    }

    @Override
    public void info( String format, Object arg )
    {
        log.info( format, arg );
    }

    @Override
    public void info( String format, Object argA, Object argB )
    {
        log.info( format, argA, argB );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        log.info( format, arguments );
    }

    @Override
    public void info( String msg, Throwable t )
    {
        log.info( "%s%n%s", msg, t );
    }

    @Override
    public void info( Throwable t )
    {
        info( t.getMessage(), t );
    }

    @Override
    public boolean isWarnEnabled()
    {
        return true;
    }

    @Override
    public void warn( String msg )
    {
        log.warn( msg );
    }

    @Override
    public void warn( String format, Object arg )
    {
        log.warn( format, arg );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        log.warn( format, arguments );
    }

    @Override
    public void warn( String format, Object argA, Object argB )
    {
        log.warn( format, argA, argB );
    }

    @Override
    public void warn( String msg, Throwable t )
    {
        log.warn( "%s%n%s", msg, t );
    }

    @Override
    public void warn( Throwable t )
    {
        warn( t.getMessage(), t );
    }

    @Override
    public boolean isErrorEnabled()
    {
        return true;
    }

    @Override
    public void error( String msg )
    {
        log.error( msg, null );
    }

    @Override
    public void error( String format, Object arg )
    {
        error( format(format, arg) );
    }

    @Override
    public void error( String format, Object argA, Object argB )
    {
        error( format(format, argA, argB) );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        error( format(format, arguments) );
    }

    @Override
    public void error( String msg, Throwable t )
    {
        log.error( msg, t );
    }

    @Override
    public void error( Throwable t )
    {
        error( t.getMessage(), t );
    }

    @Override
    public boolean isEnabled( InternalLogLevel level )
    {
        if ( level == InternalLogLevel.TRACE )
        {
            return isTraceEnabled();
        }
        else if ( level == InternalLogLevel.DEBUG )
        {
            return isDebugEnabled();
        }
        else
        {
            return true;
        }
    }

    @Override
    public void log( InternalLogLevel level, String msg )
    {
        switch ( level )
        {
        case TRACE:
            trace( msg );
            break;
        case DEBUG:
            debug( msg );
            break;
        case INFO:
            info( msg );
            break;
        case WARN:
            warn( msg );
            break;
        case ERROR:
            error( msg );
            break;
        }
    }

    @Override
    public void log( InternalLogLevel level, String format, Object arg )
    {
        switch ( level )
        {
        case TRACE:
            trace( format, arg );
            break;
        case DEBUG:
            debug( format, arg );
            break;
        case INFO:
            info( format, arg );
            break;
        case WARN:
            warn( format, arg );
            break;
        case ERROR:
            error( format, arg );
            break;
        }
    }

    @Override
    public void log( InternalLogLevel level, String format, Object argA, Object argB )
    {
        switch ( level )
        {
        case TRACE:
            trace( format, argA, argB );
            break;
        case DEBUG:
            debug( format, argA, argB );
            break;
        case INFO:
            info( format, argA, argB );
            break;
        case WARN:
            warn( format, argA, argB );
            break;
        case ERROR:
            error( format, argA, argB );
            break;
        }
    }

    @Override
    public void log( InternalLogLevel level, String format, Object... arguments )
    {
        switch ( level )
        {
        case TRACE:
            trace( format, arguments );
            break;
        case DEBUG:
            debug( format, arguments );
            break;
        case INFO:
            info( format, arguments );
            break;
        case WARN:
            warn( format, arguments );
            break;
        case ERROR:
            error( format, arguments );
            break;
        }
    }

    @Override
    public void log( InternalLogLevel level, String msg, Throwable t )
    {
        switch ( level )
        {
        case TRACE:
            trace( msg, t );
            break;
        case DEBUG:
            debug( msg, t );
            break;
        case INFO:
            info( msg, t );
            break;
        case WARN:
            warn( msg, t );
            break;
        case ERROR:
            error( msg, t );
            break;
        }
    }

    @Override
    public void log( InternalLogLevel level, Throwable t )
    {
        switch ( level )
        {
        case TRACE:
            trace( t );
            break;
        case DEBUG:
            debug( t );
            break;
        case INFO:
            info( t );
            break;
        case WARN:
            warn( t );
            break;
        case ERROR:
            error( t );
            break;
        }
    }
}
