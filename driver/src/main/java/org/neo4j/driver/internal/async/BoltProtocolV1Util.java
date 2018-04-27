/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.async;

import io.netty.buffer.ByteBuf;

import static io.netty.buffer.Unpooled.copyInt;
import static io.netty.buffer.Unpooled.unreleasableBuffer;

public final class BoltProtocolV1Util
{
    public static final int HTTP = 1213486160; //== 0x48545450 == "HTTP"

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    public static final int PROTOCOL_VERSION_1 = 1;
    public static final int NO_PROTOCOL_VERSION = 0;

    public static final int CHUNK_HEADER_SIZE_BYTES = 2;

    public static final int DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES = Short.MAX_VALUE / 2;

    private static final ByteBuf BOLT_V1_HANDSHAKE_BUF = unreleasableBuffer( copyInt(
            BOLT_MAGIC_PREAMBLE,
            PROTOCOL_VERSION_1,
            NO_PROTOCOL_VERSION,
            NO_PROTOCOL_VERSION,
            NO_PROTOCOL_VERSION ) ).asReadOnly();

    private BoltProtocolV1Util()
    {
    }

    public static ByteBuf handshakeBuf()
    {
        return BOLT_V1_HANDSHAKE_BUF.duplicate();
    }

    public static String handshakeString()
    {
        return "[0x6060B017, 1, 0, 0, 0]";
    }

    public static void writeMessageBoundary( ByteBuf buf )
    {
        buf.writeShort( 0 );
    }

    public static void writeEmptyChunkHeader( ByteBuf buf )
    {
        buf.writeShort( 0 );
    }

    public static void writeChunkHeader( ByteBuf buf, int chunkStartIndex, int headerValue )
    {
        buf.setShort( chunkStartIndex, headerValue );
    }
}
