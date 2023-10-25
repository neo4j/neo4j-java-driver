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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.logging.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.logging.ConsoleLogging.ConsoleLogger;

class ConsoleLoggingTest {
    private static final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private static PrintStream sysErr;

    @BeforeAll
    static void saveSysOut() {
        sysErr = System.err;
        System.setErr(new PrintStream(out));
    }

    @AfterAll
    static void restoreSysOut() {
        System.setErr(sysErr);
    }

    @BeforeEach
    void setup() {
        out.reset();
    }

    @Test
    void shouldOnlyRecordMessageOnce() {
        // Given
        var logging = new ConsoleLogging(Level.ALL);
        var catLogger = logging.getLog("Cat");
        var dogLogger = logging.getLog("Dog");

        catLogger.debug("Meow");
        dogLogger.debug("Wow");

        var scanner = new Scanner(new ByteArrayInputStream(out.toByteArray()));
        assertTrue(scanner.hasNextLine());
        assertTrue(scanner.nextLine().contains("Meow"));
        assertTrue(scanner.hasNextLine());
        assertTrue(scanner.nextLine().contains("Wow"));
        assertFalse(scanner.hasNextLine());
    }

    @Test
    void shouldResetLoggerLevel() {
        // Given
        var logName = ConsoleLogging.class.getName();
        var logger = java.util.logging.Logger.getLogger(logName);

        // Then & When
        new ConsoleLogger(logName, Level.ALL).debug("Meow");
        assertEquals(Level.ALL, logger.getLevel());

        new ConsoleLogger(logName, Level.SEVERE).debug("Wow");
        assertEquals(Level.SEVERE, logger.getLevel());

        var scanner = new Scanner(new ByteArrayInputStream(out.toByteArray()));
        assertTrue(scanner.hasNextLine());
        assertTrue(scanner.nextLine().contains("Meow"));
        assertFalse(scanner.hasNextLine());
    }
}
