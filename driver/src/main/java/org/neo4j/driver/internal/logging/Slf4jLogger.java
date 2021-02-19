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
package org.neo4j.driver.internal.logging;

import java.util.Objects;

import org.neo4j.driver.Logger;

class Slf4jLogger implements Logger
{
    private final org.slf4j.Logger delegate;

    Slf4jLogger( org.slf4j.Logger delegate )
    {
        this.delegate = Objects.requireNonNull( delegate );
    }

    @Override
    public void error( String message, Throwable cause )
    {
        if ( delegate.isErrorEnabled() )
        {
            delegate.error( message, cause );
        }
    }

    @Override
    public void info( String message, Object... params )
    {
        if ( delegate.isInfoEnabled() )
        {
            delegate.info( formatMessage( message, params ) );
        }
    }

    @Override
    public void warn( String message, Object... params )
    {
        if ( delegate.isWarnEnabled() )
        {
            delegate.warn( formatMessage( message, params ) );
        }
    }

    @Override
    public void warn( String message, Throwable cause )
    {
        if ( delegate.isWarnEnabled() )
        {
            delegate.warn( message, cause );
        }
    }

    @Override
    public void debug( String message, Object... params )
    {
        if ( isDebugEnabled() )
        {
            delegate.debug( formatMessage( message, params ) );
        }
    }

    @Override
    public void trace( String message, Object... params )
    {
        if ( isTraceEnabled() )
        {
            delegate.trace( formatMessage( message, params ) );
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

    /**
     * Creates a fully formatted message. Such formatting is needed because driver uses {@link String#format(String, Object...)} parameters in message
     * templates, i.e. '%s' or '%d' while SLF4J uses '{}'. Thus this logger passes fully formatted messages to SLF4J.
     *
     * @param messageTemplate the message template.
     * @param params the parameters.
     * @return fully formatted message string.
     */
    private static String formatMessage( String messageTemplate, Object... params )
    {
        return String.format( messageTemplate, params );
    }
}
