/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.util.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.internal.async.inbound.ByteBufInput;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.io.ByteBufOutput;
import org.neo4j.driver.Value;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.value;

public abstract class AbstractMessageReaderTestBase
{
    @Test
    void shouldReadSuccessMessage() throws Exception
    {
        Map<String,Value> metadata = singletonMap( "hello", value( "world" ) );

        ResponseMessageHandler handler = testMessageReading( new SuccessMessage( metadata ) );

        verify( handler ).handleSuccessMessage( metadata );
    }

    @Test
    void shouldReadFailureMessage() throws Exception
    {
        ResponseMessageHandler handler = testMessageReading( new FailureMessage( "Hello", "World" ) );

        verify( handler ).handleFailureMessage( "Hello", "World" );
    }

    @Test
    void shouldReadIgnoredMessage() throws Exception
    {
        ResponseMessageHandler handler = testMessageReading( IgnoredMessage.IGNORED );

        verify( handler ).handleIgnoredMessage();
    }

    @Test
    void shouldReadRecordMessage() throws Exception
    {
        Value[] fields = {value( 1 ), value( 2 ), value( "42" )};

        ResponseMessageHandler handler = testMessageReading( new RecordMessage( fields ) );

        verify( handler ).handleRecordMessage( fields );
    }

    @Test
    void shouldFailToReadUnknownMessage()
    {
        assertThrows( IOException.class, () -> testMessageReading( DiscardAllMessage.DISCARD_ALL ) );
        assertThrows( IOException.class, () -> testMessageReading( new RunMessage( "RETURN 42" ) ) );
    }

    protected abstract MessageFormat.Reader newReader( PackInput input );

    protected ResponseMessageHandler testMessageReading( Message message ) throws IOException
    {
        PackInput input = newInputWith( message );
        MessageFormat.Reader reader = newReader( input );

        ResponseMessageHandler handler = mock( ResponseMessageHandler.class );
        reader.read( handler );

        return handler;
    }

    private static PackInput newInputWith( Message message ) throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();

        MessageFormat messageFormat = new KnowledgeableMessageFormat();
        MessageFormat.Writer writer = messageFormat.newWriter( new ByteBufOutput( buffer ) );
        writer.write( message );

        ByteBufInput input = new ByteBufInput();
        input.start( buffer );
        return input;
    }
}
