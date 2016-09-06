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
package org.neo4j.driver.internal.net;

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.v1.Value;

import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.ofValue;

public class LoggingResponseHandlerTest
{

    private String log;

    private LoggingResponseHandler handler = new LoggingResponseHandler( new DevNullLogger()
    {
        @Override
        public void debug( String message, Object... params )
        {
            log = String.format( message, params );
        }
    } );

    @Test
    public void shouldLogInitMessage() throws Throwable
    {
        // When
        handler.handleInitMessage( "client", parameters().asMap( ofValue()));

        // Then
        assertEquals( "S: INIT \"client\" {...}", log );
        assertEquals( format( new InitMessage( "client", parameters().asMap( ofValue()) ) ), log );
    }

    @Test
    public void shouldLogRunMessage() throws Throwable
    {
        // When
        handler.handleRunMessage( "stat", parameters( "value", new String[]{"cat", "cat", "cat"} ).asMap( ofValue()) );

        // Then
        assertEquals( "S: RUN \"stat\" {value=[\"cat\", \"cat\", \"cat\"]}", log );
        assertEquals( format( new RunMessage( "stat", parameters( "value", new String[]{"cat", "cat", "cat"} ).asMap(
                ofValue()) ) ),
                log );
    }

    @Test
    public void shouldLogPullAllMessage() throws Throwable
    {
        // When
        handler.handlePullAllMessage();

        // Then
        assertEquals( "S: PULL_ALL", log );
        assertEquals( format( new PullAllMessage() ), log );
    }


    @Test
    public void shouldLogDiscardAllMessage() throws Throwable
    {
        // When
        handler.handleDiscardAllMessage();
        // Then
        assertEquals( "S: DISCARD_ALL", log );
        assertEquals( format( new DiscardAllMessage() ), log );
    }

    @Test
    public void shouldLogAckFailureMessage() throws Throwable
    {
        // When
        handler.handleResetMessage();

        // Then
        assertEquals( "S: RESET", log );
        assertEquals( format( new ResetMessage() ), log );
    }

    @Test
    public void shouldLogSuccessMessage() throws Throwable
    {
        // When
        handler.appendResultCollector( Collector.NO_OP );
        handler.handleSuccessMessage( new HashMap<String,Value>() );

        // Then
        assertEquals( "S: SUCCESS {}", log );
        assertEquals( format( new SuccessMessage( new HashMap<String,Value>() ) ), log );
    }

    @Test
    public void shouldLogRecordMessage() throws Throwable
    {
        // When
        handler.appendResultCollector( Collector.NO_OP );
        handler.handleRecordMessage( new Value[]{} );

        // Then
        assertEquals( "S: RECORD []", log );
        assertEquals( format( new RecordMessage( new Value[]{} ) ), log );
    }

    @Test
    public void shouldLogFailureMessage() throws Throwable
    {
        // When
        handler.appendResultCollector( Collector.NO_OP );
        handler.handleFailureMessage( "code.error", "message" );

        // Then
        assertEquals( "S: FAILURE code.error \"message\"", log );
        assertEquals( format( new FailureMessage( "code.error", "message" ) ), log );
    }

    @Test
    public void shouldLogIgnoredMessage() throws Throwable
    {
        // When
        handler.appendResultCollector( Collector.NO_OP );
        handler.handleIgnoredMessage();

        // Then
        assertEquals( "S: IGNORED {}", log );
        assertEquals( format( new IgnoredMessage() ), log );
    }


    private String format( Message message )
    {
        return String.format( "S: %s", message );
    }
}
