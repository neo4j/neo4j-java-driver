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

import org.neo4j.driver.v1.Logger;

import static java.util.Objects.requireNonNull;

public class DelegatingLogger implements Logger
{
    private final Logger delegate;
    private final String messagePrefix;

    public DelegatingLogger( Logger delegate )
    {
        this( delegate, null );
    }

    public DelegatingLogger( Logger delegate, String messagePrefix )
    {
        this.delegate = requireNonNull( delegate );
        this.messagePrefix = messagePrefix;
    }

    @Override
    public void error( String message, Throwable cause )
    {
        delegate.error( messageWithPrefix( message ), cause );
    }

    @Override
    public void info( String message, Object... params )
    {
        delegate.info( messageWithPrefix( message ), params );
    }

    @Override
    public void warn( String message, Object... params )
    {
        delegate.warn( messageWithPrefix( message ), params );
    }

    @Override
    public void debug( String message, Object... params )
    {
        if ( isDebugEnabled() )
        {
            delegate.debug( messageWithPrefix( message ), params );
        }
    }

    @Override
    public void trace( String message, Object... params )
    {
        if ( isTraceEnabled() )
        {
            delegate.trace( messageWithPrefix( message ), params );
        }
    }

    @Override
    public boolean isTraceEnabled()
    {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled()
    {
        return delegate.isDebugEnabled();
    }

    private String messageWithPrefix( String message )
    {
        if ( messagePrefix == null )
        {
            return message;
        }
        return String.format( "[%s] %s", messagePrefix, message );
    }
}
