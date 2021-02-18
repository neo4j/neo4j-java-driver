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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.Value;

import static java.util.Objects.requireNonNull;

public class HandshakeCompletedListener implements ChannelFutureListener
{
    private final String userAgent;
    private final AuthToken authToken;
    private final RoutingContext routingContext;
    private final ChannelPromise connectionInitializedPromise;

    public HandshakeCompletedListener( String userAgent, AuthToken authToken,
                                       RoutingContext routingContext, ChannelPromise connectionInitializedPromise )
    {
        this.userAgent = requireNonNull( userAgent );
        this.authToken = requireNonNull( authToken );
        this.routingContext = routingContext;
        this.connectionInitializedPromise = requireNonNull( connectionInitializedPromise );
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        if ( future.isSuccess() )
        {
            BoltProtocol protocol = BoltProtocol.forChannel( future.channel() );
            protocol.initializeChannel( userAgent, authToken, routingContext, connectionInitializedPromise );
        }
        else
        {
            connectionInitializedPromise.setFailure( future.cause() );
        }
    }
}
