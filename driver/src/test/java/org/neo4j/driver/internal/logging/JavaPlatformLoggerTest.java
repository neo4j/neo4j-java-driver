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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

class JavaPlatformLoggerTest {
    private final Logger logger = Mockito.mock(Logger.class);
    private final JavaPlatformLogger javaPlatformLogger = new JavaPlatformLogger(logger);

    @Test
    void shouldLogErrorWithMessageAndThrowable() {
        Mockito.when(logger.isLoggable(Level.ERROR)).thenReturn(true);
        String message = "Hello";
        IllegalArgumentException error = new IllegalArgumentException("World");

        javaPlatformLogger.error(message, error);

        Mockito.verify(logger).log(Level.ERROR, message, error);
    }

    @Test
    void shouldLogInfoWithMessageAndParams() {
        Mockito.when(logger.isLoggable(Level.INFO)).thenReturn(true);
        String message = "One %s, two %s, three %s";
        Object[] params = {"111", "222", "333"};

        javaPlatformLogger.info(message, params);

        Mockito.verify(logger).log(Level.INFO, "One 111, two 222, three 333");
    }

    @Test
    void shouldLogWarnWithMessageAndParams() {
        Mockito.when(logger.isLoggable(Level.WARNING)).thenReturn(true);
        String message = "C for %s, d for %s";
        Object[] params = {"cat", "dog"};

        javaPlatformLogger.warn(message, params);

        Mockito.verify(logger).log(Level.WARNING, "C for cat, d for dog");
    }

    @Test
    void shouldLogWarnWithMessageAndThrowable() {
        Mockito.when(logger.isLoggable(Level.WARNING)).thenReturn(true);
        String message = "Hello";
        RuntimeException error = new RuntimeException("World");

        javaPlatformLogger.warn(message, error);

        Mockito.verify(logger).log(Level.WARNING, message, error);
    }

    @Test
    void shouldLogDebugWithMessageAndParams() {
        Mockito.when(logger.isLoggable(Level.DEBUG)).thenReturn(true);
        String message = "Hello%s%s!";
        Object[] params = {" ", "World"};

        javaPlatformLogger.debug(message, params);

        Mockito.verify(logger).log(Level.DEBUG, "Hello World!");
    }

    @Test
    void shouldLogTraceWithMessageAndParams() {
        Mockito.when(logger.isLoggable(Level.TRACE)).thenReturn(true);
        String message = "I'll be %s!";
        Object[] params = {"back"};

        javaPlatformLogger.trace(message, params);

        Mockito.verify(logger).log(Level.TRACE, "I'll be back!");
    }

    @Test
    void shouldCheckIfDebugIsEnabled() {
        Mockito.when(logger.isLoggable(Level.DEBUG)).thenReturn(false);
        assertFalse(javaPlatformLogger.isDebugEnabled());

        Mockito.when(logger.isLoggable(Level.DEBUG)).thenReturn(true);
        assertTrue(javaPlatformLogger.isDebugEnabled());
    }

    @Test
    void shouldCheckIfTraceIsEnabled() {
        Mockito.when(logger.isLoggable(Level.TRACE)).thenReturn(false);
        assertFalse(javaPlatformLogger.isTraceEnabled());

        Mockito.when(logger.isLoggable(Level.TRACE)).thenReturn(true);
        assertTrue(javaPlatformLogger.isTraceEnabled());
    }
}
