/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import io.netty.buffer.ByteBuf;

import org.neo4j.driver.internal.messaging.v1.BoltProtocolV1;
import org.neo4j.driver.internal.messaging.v2.BoltProtocolV2;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;

import static io.netty.buffer.Unpooled.copyInt;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static java.lang.Integer.toHexString;

public final class BoltProtocolUtil
{
    public static final int HTTP = 1213486160; //== 0x48545450 == "HTTP"

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    public static final int NO_PROTOCOL_VERSION = 0;

    public static final int CHUNK_HEADER_SIZE_BYTES = 2;

    public static final int DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES = Short.MAX_VALUE / 2;

    private static final ByteBuf HANDSHAKE_BUF = unreleasableBuffer( copyInt(
            BOLT_MAGIC_PREAMBLE,
            BoltProtocolV4.VERSION,
            BoltProtocolV3.VERSION,
            BoltProtocolV2.VERSION,
            BoltProtocolV1.VERSION ) ).asReadOnly();

    private static final String HANDSHAKE_STRING = createHandshakeString();

    private BoltProtocolUtil()
    {
    }

    public static ByteBuf handshakeBuf()
    {
        return HANDSHAKE_BUF.duplicate();
    }

    public static String handshakeString()
    {
        return HANDSHAKE_STRING;
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

    private static String createHandshakeString()
    {
        ByteBuf buf = handshakeBuf();
        return String.format( "[0x%s, %s, %s, %s, %s]", toHexString( buf.readInt() ), buf.readInt(), buf.readInt(), buf.readInt(), buf.readInt() );
    }
}
