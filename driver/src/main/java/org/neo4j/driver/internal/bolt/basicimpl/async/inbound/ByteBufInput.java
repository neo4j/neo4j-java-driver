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

import static java.util.Objects.requireNonNull;

import io.netty.buffer.ByteBuf;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;

public class ByteBufInput implements PackInput {
    private ByteBuf buf;

    public void start(ByteBuf newBuf) {
        assertNotStarted();
        buf = requireNonNull(newBuf);
    }

    public void stop() {
        buf = null;
    }

    @Override
    public byte readByte() {
        return buf.readByte();
    }

    @Override
    public short readShort() {
        return buf.readShort();
    }

    @Override
    public int readInt() {
        return buf.readInt();
    }

    @Override
    public long readLong() {
        return buf.readLong();
    }

    @Override
    public double readDouble() {
        return buf.readDouble();
    }

    @Override
    public void readBytes(byte[] into, int offset, int toRead) {
        buf.readBytes(into, offset, toRead);
    }

    @Override
    public byte peekByte() {
        return buf.getByte(buf.readerIndex());
    }

    private void assertNotStarted() {
        if (buf != null) {
            throw new IllegalStateException("Already started");
        }
    }
}
