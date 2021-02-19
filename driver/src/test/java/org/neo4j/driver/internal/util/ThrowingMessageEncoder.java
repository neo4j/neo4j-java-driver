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
package org.neo4j.driver.internal.util;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;

public class ThrowingMessageEncoder<T> extends MessageToMessageEncoder<T>
{
    private final RuntimeException error;

    private ThrowingMessageEncoder( Class<T> messageType, RuntimeException error )
    {
        super( messageType );
        this.error = error;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, T msg, List<Object> out )
    {
        ctx.pipeline().remove( this );
        throw error;
    }

    public static ThrowingMessageEncoder<RunMessage> forRunMessage( RuntimeException error )
    {
        return new ThrowingMessageEncoder<>( RunMessage.class, error );
    }

    public static ThrowingMessageEncoder<RunWithMetadataMessage> forRunWithMetadataMessage( RuntimeException error )
    {
        return new ThrowingMessageEncoder<>( RunWithMetadataMessage.class, error );
    }

    public static ThrowingMessageEncoder<ResetMessage> forResetMessage( RuntimeException error )
    {
        return new ThrowingMessageEncoder<>( ResetMessage.class, error );
    }
}
