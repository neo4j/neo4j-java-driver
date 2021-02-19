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
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.util.io.ByteBufOutput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;

public abstract class AbstractMessageWriterTestBase
{
    @TestFactory
    Stream<DynamicNode> shouldWriteSupportedMessages()
    {
        return supportedMessages().map( message ->
                dynamicTest( message.toString(), () -> testSupportedMessageWriting( message ) ) );
    }

    @TestFactory
    Stream<DynamicNode> shouldFailToWriteUnsupportedMessages()
    {
        return unsupportedMessages().map( message ->
                dynamicTest( message.toString(), () -> testUnsupportedMessageWriting( message ) ) );
    }

    protected abstract MessageFormat.Writer newWriter( PackOutput output );

    protected abstract Stream<Message> supportedMessages();

    protected abstract Stream<Message> unsupportedMessages();

    private void testSupportedMessageWriting( Message message ) throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        PackOutput output = new ByteBufOutput( buffer );

        MessageFormat.Writer writer = newWriter( output );
        writer.write( message );

        ByteBufInput input = new ByteBufInput();
        input.start( buffer );
        PackStream.Unpacker unpacker = new PackStream.Unpacker( input );

        long structHeader = unpacker.unpackStructHeader();
        assertThat( structHeader, greaterThanOrEqualTo( 0L ) );

        byte structSignature = unpacker.unpackStructSignature();
        assertEquals( message.signature(), structSignature );
    }

    private void testUnsupportedMessageWriting( Message message )
    {
        MessageFormat.Writer writer = newWriter( mock( PackOutput.class ) );
        assertThrows( Exception.class, () -> writer.write( message ) );
    }
}
