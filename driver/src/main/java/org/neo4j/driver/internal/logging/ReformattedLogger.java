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

import org.neo4j.driver.Logger;

import static java.util.Objects.requireNonNull;

public abstract class ReformattedLogger implements Logger
{
    private final Logger delegate;

    protected ReformattedLogger(Logger delegate)
    {
        this.delegate = requireNonNull( delegate );
    }

    @Override
    public void error( String message, Throwable cause )
    {
        delegate.error( reformat( message ), cause );
    }

    @Override
    public void info( String message, Object... params )
    {
        delegate.info( reformat( message ), params );
    }

    @Override
    public void warn( String message, Object... params )
    {
        delegate.warn( reformat( message ), params );
    }

    @Override
    public void warn( String message, Throwable cause )
    {
        delegate.warn( reformat( message ), cause );
    }

    @Override
    public void debug( String message, Object... params )
    {
        if ( isDebugEnabled() )
        {
            delegate.debug( reformat( message ), params );
        }
    }

    @Override
    public void trace( String message, Object... params )
    {
        if ( isTraceEnabled() )
        {
            delegate.trace( reformat( message ), params );
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

    protected abstract String reformat( String message );
}
