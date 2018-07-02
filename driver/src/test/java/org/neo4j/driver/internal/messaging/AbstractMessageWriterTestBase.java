/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.driver.internal.async.inbound.ByteBufInput;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.util.ByteBufOutput;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractMessageWriterTestBase
{
    @Test
    void shouldWriteDiscardAllMessage() throws Exception
    {
        testMessageWriting( DiscardAllMessage.DISCARD_ALL, 0 );
    }

    @Test
    void shouldWriteInitMessage() throws Exception
    {
        testMessageWriting( new InitMessage( "MyDriver", emptyMap() ), 2 );
    }

    @Test
    void shouldWritePullAllMessage() throws Exception
    {
        testMessageWriting( PullAllMessage.PULL_ALL, 0 );
    }

    @Test
    void shouldWriteResetMessage() throws Exception
    {
        testMessageWriting( ResetMessage.RESET, 0 );
    }

    @Test
    void shouldWriteRunMessage() throws Exception
    {
        testMessageWriting( new RunMessage( "RETURN 1", emptyMap() ), 2 );
    }

    protected abstract MessageFormat.Writer newWriter( PackOutput output );

    protected void testMessageWriting( Message message, int expectedStructHeader ) throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        PackOutput output = new ByteBufOutput( buffer );

        MessageFormat.Writer writer = newWriter( output );
        writer.write( message );

        ByteBufInput input = new ByteBufInput();
        input.start( buffer );
        PackStream.Unpacker unpacker = new PackStream.Unpacker( input );

        long structHeader = unpacker.unpackStructHeader();
        assertEquals( expectedStructHeader, structHeader );

        byte structSignature = unpacker.unpackStructSignature();
        assertEquals( message.signature(), structSignature );
    }
}
