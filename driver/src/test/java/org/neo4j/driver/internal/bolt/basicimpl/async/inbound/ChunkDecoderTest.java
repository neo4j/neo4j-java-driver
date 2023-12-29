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
package org.neo4j.driver.internal.bolt.basicimpl.async.inbound;

import static io.netty.buffer.ByteBufUtil.hexDump;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copyShort;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.testutil.TestUtil.assertByteBufEquals;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ResourceBundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;

class ChunkDecoderTest {
    private ByteBuf buffer;
    private EmbeddedChannel channel = new EmbeddedChannel(newChunkDecoder());

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.release(buffer.refCnt());
        }
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldDecodeFullChunk() {
        // whole chunk with header and body arrives at once
        var input = buffer();
        input.writeShort(7);
        input.writeByte(1);
        input.writeByte(11);
        input.writeByte(2);
        input.writeByte(22);
        input.writeByte(3);
        input.writeByte(33);
        input.writeByte(4);

        // after buffer is written there should be something to read on the other side
        assertTrue(channel.writeInbound(input));
        assertTrue(channel.finish());

        // there should only be a single chunk available for reading
        assertEquals(1, channel.inboundMessages().size());
        // it should have no size header and expected body
        assertByteBufEquals(input.slice(2, 7), channel.readInbound());
    }

    @Test
    void shouldDecodeSplitChunk() {
        // first part of the chunk contains size header and some bytes
        var input1 = buffer();
        input1.writeShort(9);
        input1.writeByte(1);
        input1.writeByte(11);
        input1.writeByte(2);
        // nothing should be available for reading
        assertFalse(channel.writeInbound(input1));

        // second part contains just a single byte
        var input2 = buffer();
        input2.writeByte(22);
        // nothing should be available for reading
        assertFalse(channel.writeInbound(input2));

        // third part contains couple more bytes
        var input3 = buffer();
        input3.writeByte(3);
        input3.writeByte(33);
        input3.writeByte(4);
        // nothing should be available for reading
        assertFalse(channel.writeInbound(input3));

        // fourth part contains couple more bytes, and the chunk is now complete
        var input4 = buffer();
        input4.writeByte(44);
        input4.writeByte(5);
        // there should be something to read now
        assertTrue(channel.writeInbound(input4));

        assertTrue(channel.finish());

        // there should only be a single chunk available for reading
        assertEquals(1, channel.inboundMessages().size());
        // it should have no size header and expected body
        assertByteBufEquals(wrappedBuffer(new byte[] {1, 11, 2, 22, 3, 33, 4, 44, 5}), channel.readInbound());
    }

    @Test
    void shouldDecodeEmptyChunk() {
        // chunk contains just the size header which is zero
        var input = copyShort(0);
        assertTrue(channel.writeInbound(input));
        assertTrue(channel.finish());

        // there should only be a single chunk available for reading
        assertEquals(1, channel.inboundMessages().size());
        // it should have no size header and empty body
        assertByteBufEquals(wrappedBuffer(new byte[0]), channel.readInbound());
    }

    @Test
    void shouldLogEmptyChunkOnTraceLevel() {
        var logger = newTraceLogger();
        channel = new EmbeddedChannel(new ChunkDecoder(newLogging(logger)));

        buffer = copyShort(0);
        assertTrue(channel.writeInbound(buffer.copy())); // copy buffer so we can verify against it later
        assertTrue(channel.finish());

        var messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(logger)
                .log(eq(System.Logger.Level.TRACE), eq((ResourceBundle) null), anyString(), messageCaptor.capture());

        // pretty hex dump should be logged
        assertEquals(hexDump(buffer), messageCaptor.getValue());
        // single empty chunk should be available for reading
        assertEquals(1, channel.inboundMessages().size());
        assertByteBufEquals(wrappedBuffer(new byte[0]), channel.readInbound());
    }

    @Test
    void shouldLogNonEmptyChunkOnTraceLevel() {
        var logger = newTraceLogger();
        channel = new EmbeddedChannel(new ChunkDecoder(newLogging(logger)));

        var bytes = "Hello".getBytes();
        buffer = buffer();
        buffer.writeShort(bytes.length);
        buffer.writeBytes(bytes);

        assertTrue(channel.writeInbound(buffer.copy())); // copy buffer so we can verify against it later
        assertTrue(channel.finish());

        var messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(logger)
                .log(eq(System.Logger.Level.TRACE), eq((ResourceBundle) null), anyString(), messageCaptor.capture());

        // pretty hex dump should be logged
        assertEquals(hexDump(buffer), messageCaptor.getValue());
        // single chunk should be available for reading
        assertEquals(1, channel.inboundMessages().size());
        assertByteBufEquals(wrappedBuffer(bytes), channel.readInbound());
    }

    @Test
    public void shouldDecodeMaxSizeChunk() {
        var message = new byte[0xFFFF];

        var input = buffer();
        input.writeShort(message.length); // chunk header
        input.writeBytes(message); // chunk body

        assertTrue(channel.writeInbound(input));
        assertTrue(channel.finish());

        assertEquals(1, channel.inboundMessages().size());
        assertByteBufEquals(wrappedBuffer(message), channel.readInbound());
    }

    private static ChunkDecoder newChunkDecoder() {
        return new ChunkDecoder(NoopLoggingProvider.INSTANCE);
    }

    private static System.Logger newTraceLogger() {
        var logger = mock(System.Logger.class);
        when(logger.isLoggable(System.Logger.Level.TRACE)).thenReturn(true);
        return logger;
    }

    private static LoggingProvider newLogging(System.Logger logger) {
        var logging = mock(LoggingProvider.class);
        when(logging.getLog(any(Class.class))).thenReturn(logger);
        return logging;
    }
}
