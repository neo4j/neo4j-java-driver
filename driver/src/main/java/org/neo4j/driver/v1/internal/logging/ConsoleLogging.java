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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.neo4j.driver.v1.internal.spi.Logger;
import org.neo4j.driver.v1.internal.spi.Logging;

/**
 * Print all the logging messages into {@link System#err}.
 * <p>
 * To use this class for debugging:
 * <pre>
 * {@code
 *
 *     Config config = Config.build()
 *                      .withLogging( new ConsoleLogging( Level.ALL ) )
 *                      .toConfig();
 *     Driver driver = GraphDatabase.driver( "bolt://localhost:7687", config );
 * }
 * </pre>
 */
public class ConsoleLogging implements Logging
{
    private final ConsoleLogger logger;

    public ConsoleLogging( Level level )
    {
        this.logger = new ConsoleLogger( this.getClass().getName(), level );
    }

    @Override
    public Logger getLog( String name )
    {
        return this.logger;
    }

    public static class ConsoleLogger extends JULogger
    {
        private final ConsoleHandler handler;

        public ConsoleLogger( String name, Level level )
        {
            super( name, level );
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger( name );

            logger.setUseParentHandlers( false );
            // remove all other logging handlers
            Handler[] handlers = logger.getHandlers();
            for ( int i = 0; i < handlers.length; i++ )
            {
                logger.removeHandler( handlers[i] );
            }

            handler = new ConsoleHandler();
            handler.setFormatter( new ShortFormatter() );
            handler.setLevel( level );
            logger.addHandler( handler );
            logger.setLevel( level );

        }
    }

    private static class ShortFormatter extends Formatter
    {
        private static final DateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd hh:mm:ss,SSS" );

        public String format( LogRecord record )
        {
            StringBuilder builder = new StringBuilder( 1000 );
            builder.append( dateFormat.format( new Date( record.getMillis() ) ) ).append( " - " );
            // builder.append( "[" ).append( record.getSourceClassName() ).append( "." );
            // builder.append( record.getSourceMethodName() ).append( "] - " );
            // builder.append( "[" ).append( record.getLevel() ).append( "] - " );
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
