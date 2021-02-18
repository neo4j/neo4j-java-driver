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
package org.neo4j.driver.internal.util.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.driver.internal.async.inbound.ByteBufInput;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.io.ByteBufOutput;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public abstract class AbstractMessageReaderTestBase
{
    @TestFactory
    Stream<DynamicNode> shouldReadSupportedMessages()
    {
        return supportedMessages().map( message ->
                                                dynamicTest( message.toString(), () -> testSupportedMessageReading( message ) ) );
    }

    private void testSupportedMessageReading( Message message ) throws IOException
    {
        ResponseMessageHandler handler = testMessageReading( message );

        if ( message instanceof SuccessMessage )
        {
            SuccessMessage successMessage = (SuccessMessage) message;
            verify( handler ).handleSuccessMessage( successMessage.metadata() );
        }
        else if ( message instanceof FailureMessage )
        {
            FailureMessage failureMessage = (FailureMessage) message;
            verify( handler ).handleFailureMessage( failureMessage.code(), failureMessage.message() );
        }
        else if ( message instanceof IgnoredMessage )
        {
            verify( handler ).handleIgnoredMessage();
        }
        else if ( message instanceof RecordMessage )
        {
            RecordMessage recordMessage = (RecordMessage) message;
            verify( handler ).handleRecordMessage( recordMessage.fields() );
        }
        else
        {
            fail( "Unsupported message type " + message.getClass().getSimpleName() );
        }
    }

    @TestFactory
    Stream<DynamicNode> shouldFailToReadUnsupportedMessages()
    {
        return unsupportedMessages().map( message ->
                                                  dynamicTest( message.toString(), () -> testUnsupportedMessageReading( message ) ) );
    }

    private void testUnsupportedMessageReading( Message message ) throws IOException
    {
        assertThrows( IOException.class, () -> testMessageReading( message ) );
    }

    protected abstract Stream<Message> supportedMessages();

    protected abstract Stream<Message> unsupportedMessages();

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
