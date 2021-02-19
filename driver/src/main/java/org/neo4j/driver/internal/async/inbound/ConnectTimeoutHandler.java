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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.exceptions.ServiceUnavailableException;

/**
 * Handler needed to limit amount of time connection performs TLS and Bolt handshakes.
 * It should only be used when connection is established and removed from the pipeline afterwards.
 * Otherwise it will make long running queries fail.
 */
public class ConnectTimeoutHandler extends ReadTimeoutHandler
{
    private final long timeoutMillis;
    private boolean triggered;

    public ConnectTimeoutHandler( long timeoutMillis )
    {
        super( timeoutMillis, TimeUnit.MILLISECONDS );
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    protected void readTimedOut( ChannelHandlerContext ctx )
    {
        if ( !triggered )
        {
            triggered = true;
            ctx.fireExceptionCaught( unableToConnectError() );
        }
    }

    private ServiceUnavailableException unableToConnectError()
    {
        return new ServiceUnavailableException( "Unable to establish connection in " + timeoutMillis + "ms" );
    }
}
