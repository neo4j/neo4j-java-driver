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
package org.neo4j.driver;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.logging.Slf4jLogging;

import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

/**
 * Accessor for {@link Logger} instances. Configured once for a driver instance using {@link Config.ConfigBuilder#withLogging(Logging)} builder method.
 * Users are expected to either implement this interface or use one of the existing implementations (available via static methods in this interface):
 * <ul>
 * <li>{@link #slf4j() SLF4J logging} - uses available SLF4J binding (Logback, Log4j, etc.) fails when no SLF4J implementation is available. Uses
 * application's logging configuration from XML or other type of configuration file. This logging method is the preferred one and relies on the SLF4J
 * implementation available in the classpath or modulepath.</li>
 * <li>{@link #javaUtilLogging(Level) Java Logging API (JUL)} - uses {@link java.util.logging.Logger} created via
 * {@link java.util.logging.Logger#getLogger(String)}. Global java util logging configuration applies. This logging method is suitable when application
 * uses JUL for logging and explicitly configures it.</li>
 * <li>{@link #console(Level) Console logging} - uses {@link ConsoleHandler} with the specified {@link Level logging level} to print messages to {@code
 * System.err}. This logging method is suitable for quick debugging or prototyping.</li>
 * <li>{@link #none() No logging} - implementation that discards all logged messages. This logging method is suitable for testing to make driver produce
 * no output.</li>
 * </ul>
 * <p>
 * Driver logging API defines the following log levels: ERROR, INFO, WARN, DEBUG and TRACE. They are similar to levels defined by SLF4J but different from
 * log levels defined for {@link java.util.logging}. The following mapping takes place:
 * <table border="1" style="border-collapse: collapse">
 * <caption>Driver and JUL log levels</caption>
 * <tr>
 * <th>Driver</th>
 * <th>java.util.logging</th>
 * </tr>
 * <tr>
 * <td>ERROR</td>
 * <td>SEVERE</td>
 * </tr>
 * <tr>
 * <td>INFO</td>
 * <td>INFO, CONFIG</td>
 * </tr>
 * <tr>
 * <td>WARN</td>
 * <td>WARNING</td>
 * </tr>
 * <tr>
 * <td>DEBUG</td>
 * <td>FINE, FINER</td>
 * </tr>
 * <tr>
 * <td>TRACE</td>
 * <td>FINEST</td>
 * </tr>
 * </table>
 * <p>
 * Example of driver configuration with SLF4J logging:
 * <pre>
 * {@code
 * Driver driver = GraphDatabase.driver("bolt://localhost:7687",
 *                                         AuthTokens.basic("neo4j", "password"),
 *                                         Config.build().withLogging(Logging.slf4j()).toConfig());
 * }
 * </pre>
 *
 * @see Logger
 * @see Config.ConfigBuilder#withLogging(Logging)
 */
public interface Logging
{
    /**
     * Obtain a {@link Logger} instance by name.
     *
     * @param name name of a {@link Logger}
     * @return {@link Logger} instance
     */
    Logger getLog( String name );

    /**
     * Create logging implementation that uses SLF4J.
     *
     * @return new logging implementation.
     * @throws IllegalStateException if SLF4J is not available.
     */
    static Logging slf4j()
    {
        RuntimeException unavailabilityError = Slf4jLogging.checkAvailability();
        if ( unavailabilityError != null )
        {
            throw unavailabilityError;
        }
        return new Slf4jLogging();
    }

    /**
     * Create logging implementation that uses {@link java.util.logging}.
     *
     * @param level the log level.
     * @return new logging implementation.
     */
    static Logging javaUtilLogging( Level level )
    {
        return new JULogging( level );
    }

    /**
     * Create logging implementation that uses {@link java.util.logging} to log to {@code System.err}.
     *
     * @param level the log level.
     * @return new logging implementation.
     */
    static Logging console( Level level )
    {
        return new ConsoleLogging( level );
    }

    /**
     * Create logging implementation that discards all messages and logs nothing.
     *
     * @return new logging implementation.
     */
    static Logging none()
    {
        return DEV_NULL_LOGGING;
    }
}
