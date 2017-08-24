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
package org.neo4j.driver.internal.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public final class ProtocolConstants
{
    public static final int HTTP = 1213486160; //== 0x48545450 == "HTTP"

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    public static final int PROTOCOL_VERSION_1 = 1;
    public static final int NO_PROTOCOL_VERSION = 0;

    public static final ByteBuf HANDSHAKE_BUF = Unpooled.unreleasableBuffer( Unpooled.copyInt(
            BOLT_MAGIC_PREAMBLE,
            PROTOCOL_VERSION_1,
            NO_PROTOCOL_VERSION,
            NO_PROTOCOL_VERSION,
            NO_PROTOCOL_VERSION ) );

    public static final ByteBuf MESSAGE_BOUNDARY = Unpooled.unreleasableBuffer( Unpooled.copyShort( 0 ) );

    private ProtocolConstants()
    {
    }
}
