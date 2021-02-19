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

import org.neo4j.driver.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PrefixedLoggerTest
{
    private static final String PREFIX = "Output";
    private static final String MESSAGE = "Hello World!";
    private static final Exception ERROR = new Exception();

    @Test
    void shouldThrowWhenDelegateIsNull()
    {
        assertThrows( NullPointerException.class, () -> new PrefixedLogger( null ) );
    }

    @Test
    void shouldAllowNullPrefix()
    {
        assertNotNull( new PrefixedLogger( null, newLoggerMock() ) );
    }

    @Test
    void shouldDelegateIsDebugEnabled()
    {
        Logger delegate = newLoggerMock( true, false );

        PrefixedLogger logger = new PrefixedLogger( delegate );

        assertTrue( logger.isDebugEnabled() );
        verify( delegate ).isDebugEnabled();
    }

    @Test
    void shouldDelegateIsTraceEnabled()
    {
        Logger delegate = newLoggerMock( false, true );

        PrefixedLogger logger = new PrefixedLogger( delegate );

        assertTrue( logger.isTraceEnabled() );
        verify( delegate ).isTraceEnabled();
    }

    @Test
    void shouldNotDelegateDebugLogWhenDebugDisabled()
    {
        Logger delegate = newLoggerMock();

        PrefixedLogger logger = new PrefixedLogger( delegate );
        logger.debug( MESSAGE );

        verify( delegate, never() ).debug( anyString(), any() );
    }

    @Test
    void shouldNotDelegateTraceLogWhenTraceDisabled()
    {
        Logger delegate = newLoggerMock();

        PrefixedLogger logger = new PrefixedLogger( delegate );
        logger.trace( MESSAGE );

        verify( delegate, never() ).trace( anyString(), any() );
    }

    @Test
    void shouldDelegateErrorMessageWhenNoPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( delegate );

        logger.error( MESSAGE, ERROR );

        verify( delegate ).error( MESSAGE, ERROR );
    }

    @Test
    void shouldDelegateInfoMessageWhenNoPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( delegate );

        logger.info( MESSAGE );

        verify( delegate ).info( MESSAGE );
    }

    @Test
    void shouldDelegateWarnMessageWhenNoPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( delegate );

        logger.warn( MESSAGE );

        verify( delegate ).warn( MESSAGE );
    }

    @Test
    void shouldDelegateWarnMessageWithoutErrorWhenNoPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( delegate );

        Exception cause = new Exception();
        logger.warn( MESSAGE, cause );

        verify( delegate ).warn( MESSAGE, cause );
    }

    @Test
    void shouldDelegateDebugMessageWhenNoPrefix()
    {
        Logger delegate = newLoggerMock( true, false );
        PrefixedLogger logger = new PrefixedLogger( delegate );

        logger.debug( MESSAGE );

        verify( delegate ).debug( MESSAGE );
    }

    @Test
    void shouldDelegateTraceMessageWhenNoPrefix()
    {
        Logger delegate = newLoggerMock( false, true );
        PrefixedLogger logger = new PrefixedLogger( delegate );

        logger.trace( MESSAGE );

        verify( delegate ).trace( MESSAGE );
    }

    @Test
    void shouldDelegateErrorMessageWithPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        logger.error( MESSAGE, ERROR );

        verify( delegate ).error( "Output Hello World!", ERROR );
    }

    @Test
    void shouldDelegateInfoMessageWithPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        logger.info( MESSAGE );

        verify( delegate ).info( "Output Hello World!" );
    }

    @Test
    void shouldDelegateWarnMessageWithPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        logger.warn( MESSAGE );

        verify( delegate ).warn( "Output Hello World!" );
    }

    @Test
    void shouldDelegateWarnMessageWithErrorWithPrefix()
    {
        Logger delegate = newLoggerMock();
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        Exception cause = new Exception();
        logger.warn( MESSAGE, cause );

        verify( delegate ).warn( "Output Hello World!", cause );
    }

    @Test
    void shouldDelegateDebugMessageWithPrefix()
    {
        Logger delegate = newLoggerMock( true, false );
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        logger.debug( MESSAGE );

        verify( delegate ).debug( "Output Hello World!" );
    }

    @Test
    void shouldDelegateTraceMessageWithPrefix()
    {
        Logger delegate = newLoggerMock( false, true );
        PrefixedLogger logger = new PrefixedLogger( PREFIX, delegate );

        logger.trace( MESSAGE );

        verify( delegate ).trace( "Output Hello World!" );
    }

    private static Logger newLoggerMock()
    {
        return newLoggerMock( false, false );
    }

    private static Logger newLoggerMock( boolean debugEnabled, boolean traceEnabled )
    {
        Logger logger = mock( Logger.class );
        when( logger.isDebugEnabled() ).thenReturn( debugEnabled );
        when( logger.isTraceEnabled() ).thenReturn( traceEnabled );
        return logger;
    }
}
