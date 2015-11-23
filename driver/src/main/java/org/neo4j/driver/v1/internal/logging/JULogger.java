/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.logging;

import java.util.logging.Level;

import org.neo4j.driver.v1.internal.spi.Logger;

public class JULogger implements Logger
{
    private final java.util.logging.Logger delegate;
    private final boolean debugEnabled;
    private final boolean traceEnabled;

    public JULogger( String name, Level loggingLevel )
    {
        delegate = java.util.logging.Logger.getLogger( name );
        delegate.setLevel( loggingLevel );
        debugEnabled = delegate.isLoggable( Level.FINE );
        traceEnabled = delegate.isLoggable( Level.FINEST );
    }

    @Override
    public void error( String message, Throwable cause )
    {
        delegate.log( Level.SEVERE, message, cause );
    }

    @Override
    public void info( String format, Object... params )
    {
        delegate.log( Level.INFO, String.format( format, params ) );
    }

    @Override
    public void debug( String format, Object... params )
    {
        if( debugEnabled )
        {
            delegate.log( Level.FINE, String.format( format, params ) );
        }
    }

    @Override
    public void trace( String format, Object... params )
    {
        if( traceEnabled )
        {
            delegate.log( Level.FINEST, String.format( format, params ) );
        }
    }

    @Override
    public boolean isTraceEnabled()
    {
        return traceEnabled;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }
}
