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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.neo4j.driver.internal.spi.Logger;

import static java.lang.Boolean.*;
import static java.lang.System.*;

public class JULogger implements Logger
{
    /** On which level the logging is configged */
    private static final Level loggingLevel = Level.parse( getProperty( "org.neo4j.driver.loggingLevel", "INFO" ) );
    /**
     * Whether logging is enabled. If logging is not enabled,
     * nothing will be logged no matter what vale is given to {@code org.neo4j.driver.loggingLevel}.
     */
    private static final boolean loggingEnabled = getBoolean( "org.neo4j.driver.loggingEnabled" );

    private final java.util.logging.Logger delegate;

    private final boolean debugEnabled; // fine level
    private final boolean traceEnabled; // finest level

    private static ConsoleHandler handler; // TODO console logging or file logging?

    public JULogger( String name )
    {
        delegate = java.util.logging.Logger.getLogger( name );

        if( loggingEnabled )
        {
            delegate.setLevel( loggingLevel );
            debugEnabled = delegate.isLoggable( Level.FINE );
            traceEnabled = delegate.isLoggable( Level.FINEST );
            configLoggingHandler();
        }
        else
        {
            debugEnabled = false;
            traceEnabled = false;
        }
    }
    private void configLoggingHandler()
    {
        if( handler == null ) // init handler
        {
            handler = new ConsoleHandler();
            handler.setFormatter( new ShortFormatter() );
            handler.setLevel( loggingLevel );
        }

        Handler[] handlers = delegate.getHandlers();
        for ( int i = 0; i < handlers.length; i++ )
        {
            if ( handlers[i] == handler )
            {
                return; // if this logger already has the handler, we should not add it again
            }
        }

        delegate.addHandler( handler ); // add the handler if this logger has not got this handler
    }

    @Override
    public void trace( String message )
    {
        if ( traceEnabled )
        {
            delegate.finest( message );
        }
    }

    @Override
    public void debug( String message )
    {
        if( debugEnabled )
        {
            delegate.log( Level.FINE, message );
        }
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

    private static class ShortFormatter extends Formatter
    {
        // Create a DateFormat to format the logger timestamp.
        private static final DateFormat dateFormat = new SimpleDateFormat( "dd/MM/yyyy hh:mm:ss.SSS" );

        public String format( LogRecord record )
        {
            StringBuilder builder = new StringBuilder( 1000 );
            builder.append( dateFormat.format( new Date( record.getMillis() ) ) ).append( " - " );
            builder.append( "[" ).append( record.getSourceClassName() ).append( "." );
            builder.append( record.getSourceMethodName() ).append( "] - " );
            builder.append( "[" ).append( record.getLevel() ).append( "] - " );
            builder.append( formatMessage( record ) );
            builder.append( "\n" );
            return builder.toString();
        }

        public String getHead( Handler h )
        {
            return super.getHead( h );
        }

        public String getTail( Handler h )
        {
            return super.getTail( h );
        }
    }
}
