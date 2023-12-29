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
package org.neo4j.driver.internal.bolt.basicimpl.async.outbound;

import static java.util.Objects.requireNonNull;

import io.netty.buffer.ByteBuf;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;

public class ChunkAwareByteBufOutput implements PackOutput {
    private final int maxChunkSize;

    private ByteBuf buf;
    private int currentChunkStartIndex;
    private int currentChunkSize;

    public ChunkAwareByteBufOutput() {
        this(BoltProtocolUtil.DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES);
    }

    ChunkAwareByteBufOutput(int maxChunkSize) {
        this.maxChunkSize = verifyMaxChunkSize(maxChunkSize);
    }

    public void start(ByteBuf newBuf) {
        assertNotStarted();
        buf = requireNonNull(newBuf);
        startNewChunk(0);
    }

    public void stop() {
        writeChunkSizeHeader();
        buf = null;
        currentChunkStartIndex = 0;
        currentChunkSize = 0;
    }

    @Override
    public PackOutput writeByte(byte value) {
        ensureCanFitInCurrentChunk(1);
        buf.writeByte(value);
        currentChunkSize += 1;
        return this;
    }

    @Override
    public PackOutput writeBytes(byte[] data) {
        var offset = 0;
        var length = data.length;
        while (offset < length) {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensureCanFitInCurrentChunk(1);

            // Write as much as we can into the current chunk
            var amountToWrite = Math.min(availableBytesInCurrentChunk(), length - offset);

            buf.writeBytes(data, offset, amountToWrite);
            currentChunkSize += amountToWrite;
            offset += amountToWrite;
        }
        return this;
    }

    @Override
    public PackOutput writeShort(short value) {
        ensureCanFitInCurrentChunk(2);
        buf.writeShort(value);
        currentChunkSize += 2;
        return this;
    }

    @Override
    public PackOutput writeInt(int value) {
        ensureCanFitInCurrentChunk(4);
        buf.writeInt(value);
        currentChunkSize += 4;
        return this;
    }

    @Override
    public PackOutput writeLong(long value) {
        ensureCanFitInCurrentChunk(8);
        buf.writeLong(value);
        currentChunkSize += 8;
        return this;
    }

    @Override
    public PackOutput writeDouble(double value) {
        ensureCanFitInCurrentChunk(8);
        buf.writeDouble(value);
        currentChunkSize += 8;
        return this;
    }

    private void ensureCanFitInCurrentChunk(int numberOfBytes) {
        var targetChunkSize = currentChunkSize + numberOfBytes;
        if (targetChunkSize > maxChunkSize) {
            writeChunkSizeHeader();
            startNewChunk(buf.writerIndex());
        }
    }

    private void startNewChunk(int index) {
        currentChunkStartIndex = index;
        BoltProtocolUtil.writeEmptyChunkHeader(buf);
        currentChunkSize = BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES;
    }

    private void writeChunkSizeHeader() {
        // go to the beginning of the chunk and write the size header
        var chunkBodySize = currentChunkSize - BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES;
        BoltProtocolUtil.writeChunkHeader(buf, currentChunkStartIndex, chunkBodySize);
    }

    private int availableBytesInCurrentChunk() {
        return maxChunkSize - currentChunkSize;
    }

    private void assertNotStarted() {
        if (buf != null) {
            throw new IllegalStateException("Already started");
        }
    }

    private static int verifyMaxChunkSize(int maxChunkSize) {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("Max chunk size should be > 0, given: " + maxChunkSize);
        }
        return maxChunkSize;
    }
}
