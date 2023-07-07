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
package org.neo4j.driver.internal.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.handshakeString;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.writeChunkHeader;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.writeEmptyChunkHeader;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.writeMessageBoundary;
import static org.neo4j.driver.testutil.TestUtil.assertByteBufContains;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.messaging.v53.BoltProtocolV53;

class BoltProtocolUtilTest {
    @Test
    void shouldReturnHandshakeBuf() {
        assertByteBufContains(
                handshakeBuf(),
                BOLT_MAGIC_PREAMBLE,
                (3 << 16) | BoltProtocolV53.VERSION.toInt(),
                (2 << 16) | BoltProtocolV44.VERSION.toInt(),
                BoltProtocolV41.VERSION.toInt(),
                BoltProtocolV3.VERSION.toInt());
    }

    @Test
    void shouldReturnHandshakeString() {
        assertEquals("[0x6060b017, 197381, 132100, 260, 3]", handshakeString());
    }

    @Test
    void shouldWriteMessageBoundary() {
        var buf = Unpooled.buffer();

        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);
        writeMessageBoundary(buf);

        assertByteBufContains(buf, 1, 2, 3, (byte) 0, (byte) 0);
    }

    @Test
    void shouldWriteEmptyChunkHeader() {
        var buf = Unpooled.buffer();

        writeEmptyChunkHeader(buf);
        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);

        assertByteBufContains(buf, (byte) 0, (byte) 0, 1, 2, 3);
    }

    @Test
    void shouldWriteChunkHeader() {
        var buf = Unpooled.buffer();

        writeEmptyChunkHeader(buf);
        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);
        writeChunkHeader(buf, 0, 42);

        assertByteBufContains(buf, (short) 42, 1, 2, 3);
    }
}
