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

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Internal implementation of the console logging.
 * <b>This class should not be used directly.</b> Please use {@link Logging#console(Level)} factory method instead.
 *
 * @see Logging#console(Level)
 */
public class ConsoleLogging implements Logging
{
    private final Level level;

    public ConsoleLogging( Level level )
    {
        this.level = Objects.requireNonNull( level );
    }

    @Override
    public Logger getLog( String name )
    {
        return new ConsoleLogger( name, level );
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
            for ( Handler handlerToRemove : handlers )
            {
                logger.removeHandler( handlerToRemove );
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
        @Override
        public String format( LogRecord record )
        {
            return LocalDateTime.now().format( ISO_LOCAL_DATE_TIME ) + " " +
                   record.getLevel() + " " +
                   record.getLoggerName() + " - " +
                   formatMessage( record ) +
                   "\n";
        }
    }
}
