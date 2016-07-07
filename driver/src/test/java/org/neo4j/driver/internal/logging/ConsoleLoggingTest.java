/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.logging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.logging.Level;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.driver.internal.logging.ConsoleLogging.ConsoleLogger;
import org.neo4j.driver.v1.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConsoleLoggingTest
{

    private static ByteArrayOutputStream out = new ByteArrayOutputStream();
    private static PrintStream sysErr;

    @BeforeClass
    public static void saveSysOut()
    {
        sysErr = System.err;
        System.setErr( new PrintStream( out ) );
    }

    @AfterClass
    public static void restoreSysOut()
    {
        System.setErr( sysErr );
    }

    @Before
    public void setup()
    {
        out.reset();
    }

    @Test
    public void shouldOnlyRecordMessageOnce()
    {
        // Given
        ConsoleLogging logging = new ConsoleLogging( Level.ALL );
        Logger catLogger = logging.getLog( "Cat" );
        Logger dogLogger = logging.getLog( "Dog" );

        catLogger.debug( "Meow" );
        dogLogger.debug( "Wow" );

        Scanner scanner = new Scanner( new ByteArrayInputStream( out.toByteArray() ) );
        assertTrue( scanner.hasNextLine() );
        assertTrue( scanner.nextLine().contains( "Meow" ) );
        assertTrue( scanner.hasNextLine() );
        assertTrue( scanner.nextLine().contains( "Wow" ) );
        assertFalse( scanner.hasNextLine() );
    }

    @Test
    public void shouldResetLoggerLevel()
    {
        // Given
        String logName = ConsoleLogging.class.getName();
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger( logName );

        // Then & When
        new ConsoleLogger( logName, Level.ALL ).debug( "Meow" );
        assertEquals( Level.ALL, logger.getLevel() );

        new ConsoleLogger( logName, Level.SEVERE ).debug( "Wow" );
        assertEquals( Level.SEVERE, logger.getLevel() );

        Scanner scanner = new Scanner( new ByteArrayInputStream( out.toByteArray() ) );
        assertTrue( scanner.hasNextLine() );
        assertTrue( scanner.nextLine().contains( "Meow" ) );
        assertFalse( scanner.hasNextLine() );
    }
}
