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
package org.neo4j.driver.v1;

/**
 * Logs messages for driver activity.
 */
public interface Logger
{
    /**
     * Logs errors from this driver
     * @param message the error message
     * @param cause the cause of the error
     */
    void error( String message, Throwable cause );

    /**
     * Logs information from the driver
     * @param message the information message
     * @param params parameters used in the information message
     */
    void info( String message, Object... params );

    /**
     * Logs warnings that happened during using the driver
     * @param message the warning message
     * @param params parameters used in the warning message
     */
    void warn( String message, Object... params );

    /**
     * Logs bolt messages sent and received by this driver.
     * It is only enabled when {@link Logger#isDebugEnabled()} returns {@code True}.
     * This logging level generates a lot of log entries.
     * @param message the bolt message
     * @param params parameters used in generating the bolt message
     */
    void debug( String message, Object... params );

    /**
     * Logs binary sent and received by this driver.
     * It is only enabled when {@link Logger#isTraceEnabled()} returns {@code True}.
     * This logging level generates huge amount of log entries.
     * @param message the bolt message in hex
     * @param params parameters used in generating the hex message
     */
    void trace( String message, Object... params );

    /**
     * Return true if the trace logging level is enabled.
     * @see Logger#trace(String, Object...)
     * @return true if the trace logging level is enabled.
     */
    boolean isTraceEnabled();

    /**
     * Return true if the debug level is enabled.
     * @see Logger#debug(String, Object...)
     * @return true if the debug level is enabled.
     */
    boolean isDebugEnabled();
}
