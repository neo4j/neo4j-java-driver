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

/**
 * Logs messages for driver activity.
 * <p>
 * Some methods in this interface take a message template together with a list of parameters. These methods are expected to construct the final
 * message only if the needed logging level is enabled. Driver expects formatting to be done using {@link String#format(String, Object...)} method.
 * Thus all supplied message templates will contain "%s" as parameter placeholders. This is different from all SLF4J-compatible logging frameworks
 * where parameter placeholder is "{}". Implementations of this interface should adapt placeholders from "%s" to "{}", if required.
 */
public interface Logger
{
    /**
     * Logs errors from this driver.
     * <p>
     * Examples of errors logged using this method:
     * <ul>
     * <li>Network connection errors</li>
     * <li>DNS resolution errors</li>
     * <li>Cluster discovery errors</li>
     * </ul>
     *
     * @param message the error message.
     * @param cause the cause of the error.
     */
    void error( String message, Throwable cause );

    /**
     * Logs information from the driver.
     * <p>
     * Example of info messages logged using this method:
     * <ul>
     * <li>Driver creation and shutdown</li>
     * <li>Cluster discovery progress</li>
     * </ul>
     *
     * @param message the information message template. Can contain {@link String#format(String, Object...)}-style placeholders, like "%s".
     * @param params parameters used in the information message.
     */
    void info( String message, Object... params );

    /**
     * Logs warnings that happened when using the driver.
     * <p>
     * Example of info messages logged using this method:
     * <ul>
     * <li>Usage of deprecated APIs</li>
     * <li>Transaction retry failures</li>
     * </ul>
     *
     * @param message the warning message template. Can contain {@link String#format(String, Object...)}-style placeholders, like "%s".
     * @param params parameters used in the warning message.
     */
    void warn( String message, Object... params );

    /**
     * Logs warnings that happened during using the driver
     *
     * <p>
     * Example of info messages logged using this method:
     * <ul>
     * <li>Usage of deprecated APIs</li>
     * <li>Transaction retry failures</li>
     * </ul>
     *
     * @param message the warning message
     * @param cause the cause of the warning
     */
    void warn( String message, Throwable cause );

    /**
     * Logs bolt messages sent and received by this driver.
     * It is only enabled when {@link Logger#isDebugEnabled()} returns {@code true}.
     * This logging level generates a lot of log entries.
     * <p>
     * Example of debug messages logged using this method:
     * <ul>
     * <li>Connection pool events, like creation, acquire and release of connections</li>
     * <li>Messages sent to the database</li>
     * <li>Messages received from the database</li>
     * </ul>
     *
     * @param message the debug message template. Can contain {@link String#format(String, Object...)}-style placeholders, like "%s".
     * @param params parameters used in generating the bolt message
     */
    void debug( String message, Object... params );

    /**
     * Logs binary sent and received by this driver.
     * It is only enabled when {@link Logger#isTraceEnabled()} returns {@code true}.
     * This logging level generates huge amount of log entries.
     *
     * <p>
     * Example of debug messages logged using this method:
     * <ul>
     * <li>Idle connection pings</li>
     * <li>Server selection for load balancing</li>
     * <li>Messages sent to the database with bytes in hex</li>
     * <li>Messages received from the database with bytes in hex</li>
     * </ul>
     *
     * @param message the trace message template. Can contain {@link String#format(String, Object...)}-style placeholders, like "%s".
     * @param params parameters used in generating the hex message
     */
    void trace( String message, Object... params );

    /**
     * Return true if the trace logging level is enabled.
     *
     * @return true if the trace logging level is enabled.
     * @see Logger#trace(String, Object...)
     */
    boolean isTraceEnabled();

    /**
     * Return true if the debug level is enabled.
     *
     * @return true if the debug level is enabled.
     * @see Logger#debug(String, Object...)
     */
    boolean isDebugEnabled();
}
