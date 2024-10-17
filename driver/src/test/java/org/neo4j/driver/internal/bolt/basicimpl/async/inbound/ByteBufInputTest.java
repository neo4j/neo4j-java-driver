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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

class ByteBufInputTest {
    @Test
    void shouldThrowWhenStartedWithNullBuf() {
        var input = new ByteBufInput();

        assertThrows(NullPointerException.class, () -> input.start(null));
    }

    @Test
    void shouldThrowWhenStartedTwice() {
        var input = new ByteBufInput();
        input.start(mock(ByteBuf.class));

        assertThrows(IllegalStateException.class, () -> input.start(mock(ByteBuf.class)));
    }

    @Test
    void shouldDelegateReadByte() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.readByte()).thenReturn((byte) 42);
        input.start(buf);

        assertEquals((byte) 42, input.readByte());
    }

    @Test
    void shouldDelegateReadShort() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.readShort()).thenReturn((short) -42);
        input.start(buf);

        assertEquals((short) -42, input.readShort());
    }

    @Test
    void shouldDelegateReadInt() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.readInt()).thenReturn(15);
        input.start(buf);

        assertEquals(15, input.readInt());
    }

    @Test
    void shouldDelegateReadLong() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.readLong()).thenReturn(4242L);
        input.start(buf);

        assertEquals(4242L, input.readLong());
    }

    @Test
    void shouldDelegateReadDouble() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.readDouble()).thenReturn(42.42D);
        input.start(buf);

        assertEquals(42.42D, input.readDouble(), 0.00001);
    }

    @Test
    void shouldDelegateReadBytes() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        input.start(buf);

        input.readBytes(new byte[10], 3, 5);

        verify(buf).readBytes(new byte[10], 3, 5);
    }

    @Test
    void shouldDelegatePeekByte() {
        var input = new ByteBufInput();
        var buf = mock(ByteBuf.class);
        when(buf.getByte(anyInt())).thenReturn((byte) 42);
        input.start(buf);

        assertEquals((byte) 42, input.peekByte());
    }
}
