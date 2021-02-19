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
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Slf4jLoggerTest
{
    private final Logger logger = mock( Logger.class );
    private final Slf4jLogger slf4jLogger = new Slf4jLogger( logger );

    @Test
    void shouldLogErrorWithMessageAndThrowable()
    {
        when( logger.isErrorEnabled() ).thenReturn( true );
        String message = "Hello";
        IllegalArgumentException error = new IllegalArgumentException( "World" );

        slf4jLogger.error( message, error );

        verify( logger ).error( message, error );
    }

    @Test
    void shouldLogInfoWithMessageAndParams()
    {
        when( logger.isInfoEnabled() ).thenReturn( true );
        String message = "One %s, two %s, three %s";
        Object[] params = {"111", "222", "333"};

        slf4jLogger.info( message, params );

        verify( logger ).info( "One 111, two 222, three 333" );
    }

    @Test
    void shouldLogWarnWithMessageAndParams()
    {
        when( logger.isWarnEnabled() ).thenReturn( true );
        String message = "C for %s, d for %s";
        Object[] params = {"cat", "dog"};

        slf4jLogger.warn( message, params );

        verify( logger ).warn( "C for cat, d for dog" );
    }

    @Test
    void shouldLogWarnWithMessageAndThrowable()
    {
        when( logger.isWarnEnabled() ).thenReturn( true );
        String message = "Hello";
        RuntimeException error = new RuntimeException( "World" );

        slf4jLogger.warn( message, error );

        verify( logger ).warn( message, error );
    }

    @Test
    void shouldLogDebugWithMessageAndParams()
    {
        when( logger.isDebugEnabled() ).thenReturn( true );
        String message = "Hello%s%s!";
        Object[] params = {" ", "World"};

        slf4jLogger.debug( message, params );

        verify( logger ).debug( "Hello World!" );
    }

    @Test
    void shouldLogTraceWithMessageAndParams()
    {
        when( logger.isTraceEnabled() ).thenReturn( true );
        String message = "I'll be %s!";
        Object[] params = {"back"};

        slf4jLogger.trace( message, params );

        verify( logger ).trace( "I'll be back!" );
    }

    @Test
    void shouldCheckIfDebugIsEnabled()
    {
        when( logger.isDebugEnabled() ).thenReturn( false );
        assertFalse( slf4jLogger.isDebugEnabled() );

        when( logger.isDebugEnabled() ).thenReturn( true );
        assertTrue( slf4jLogger.isDebugEnabled() );
    }

    @Test
    void shouldCheckIfTraceIsEnabled()
    {
        when( logger.isTraceEnabled() ).thenReturn( false );
        assertFalse( slf4jLogger.isTraceEnabled() );

        when( logger.isTraceEnabled() ).thenReturn( true );
        assertTrue( slf4jLogger.isTraceEnabled() );
    }
}
