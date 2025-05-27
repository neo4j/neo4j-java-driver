/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import java.io.PrintWriter;
import java.io.Serial;
import java.io.Serializable;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

/**
 * Internal implementation of the console logging.
 * <b>This class should not be used directly.</b> Please use {@link Logging#console(Level)} factory method instead.
 *
 * @see Logging#console(Level)
 */
@SuppressWarnings("deprecation")
public class ConsoleLogging implements Logging, Serializable {
    @Serial
    private static final long serialVersionUID = 9205935204074879150L;

    private final Level level;

    public ConsoleLogging(Level level) {
        this.level = Objects.requireNonNull(level);
    }

    @Override
    public Logger getLog(String name) {
        return new ConsoleLogger(name, level);
    }

    public static class ConsoleLogger extends JULogger {

        public ConsoleLogger(String name, Level level) {
            super(name, level);
            var logger = java.util.logging.Logger.getLogger(name);

            logger.setUseParentHandlers(false);
            // remove all other logging handlers
            var handlers = logger.getHandlers();
            for (var handlerToRemove : handlers) {
                logger.removeHandler(handlerToRemove);
            }

            var handler = new ConsoleHandler();
            handler.setFormatter(new ConsoleFormatter());
            handler.setLevel(level);
            logger.addHandler(handler);
            logger.setLevel(level);
        }
    }

    private static class ConsoleFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            return LocalDateTime.now().format(ISO_LOCAL_DATE_TIME) + " " + record.getLevel()
                    + " " + record.getLoggerName()
                    + " - " + formatMessage(record)
                    + formatThrowable(record.getThrown())
                    + "\n";
        }

        private String formatThrowable(Throwable throwable) {
            var throwableString = "";
            if (throwable != null) {
                var sw = new StringWriter();
                var pw = new PrintWriter(sw);
                pw.println();
                throwable.printStackTrace(pw);
                pw.close();
                throwableString = sw.toString();
            }
            return throwableString;
        }
    }
}
