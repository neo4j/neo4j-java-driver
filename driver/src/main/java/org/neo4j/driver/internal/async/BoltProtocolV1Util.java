/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import static io.netty.buffer.Unpooled.copyShort;
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
            NO_PROTOCOL_VERSION ) )
            .asReadOnly();

    private static final ByteBuf MESSAGE_BOUNDARY_BUF = unreleasableBuffer( copyShort( 0 ) ).asReadOnly();

    private static final ByteBuf CHUNK_HEADER_PLACEHOLDER_BUF = unreleasableBuffer( copyShort( 0 ) ).asReadOnly();

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

    public static ByteBuf messageBoundary()
    {
        return MESSAGE_BOUNDARY_BUF.duplicate();
    }

    public static ByteBuf chunkHeaderPlaceholder()
    {
        return CHUNK_HEADER_PLACEHOLDER_BUF.duplicate();
    }
}
