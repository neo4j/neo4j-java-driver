/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.logging;

import java.util.logging.Level;

import org.neo4j.driver.internal.spi.Logger;

public class JULogger implements Logger
{
    private final java.util.logging.Logger delegate;

    public JULogger( String name )
    {
        delegate = java.util.logging.Logger.getLogger( name );
    }

    @Override
    public void log( Level level, String message )
    {
        delegate.log( level, message );
    }

    @Override
    public void debug( String message )
    {
        // TODO: This causes synchronized on every debug call, major performance issue
        // We should check the log level when we initialize, and not allow runtime changing it.
        delegate.log( Level.FINE, message );
    }

    @Override
    public void info( String message )
    {
        delegate.log( Level.INFO, message );
    }

    @Override
    public void error( String message, Throwable cause )
    {
        delegate.log( Level.SEVERE, message, cause );
    }
}
