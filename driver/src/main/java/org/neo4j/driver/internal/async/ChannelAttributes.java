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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import org.neo4j.driver.internal.net.BoltServerAddress;

public final class ChannelAttributes
{
    private static final AttributeKey<BoltServerAddress> ADDRESS = AttributeKey.newInstance( "address" );
    private static final AttributeKey<Long> CREATION_TIMESTAMP = AttributeKey.newInstance( "creationTimestamp" );
    private static final AttributeKey<ResponseHandlersHolder> RESPONSE_HANDLERS_HOLDER =
            AttributeKey.newInstance( "responseHandlersHolder" );

    private ChannelAttributes()
    {
    }

    public static BoltServerAddress address( Channel channel )
    {
        return get( channel, ADDRESS );
    }

    public static void setAddress( Channel channel, BoltServerAddress address )
    {
        setOnce( channel, ADDRESS, address );
    }

    public static long creationTimestamp( Channel channel )
    {
        return get( channel, CREATION_TIMESTAMP );
    }

    public static void setCreationTimestamp( Channel channel, long creationTimestamp )
    {
        setOnce( channel, CREATION_TIMESTAMP, creationTimestamp );
    }

    public static ResponseHandlersHolder responseHandlersHolder( Channel channel )
    {
        return get( channel, RESPONSE_HANDLERS_HOLDER );
    }

    public static void setResponseHandlersHolder( Channel channel, ResponseHandlersHolder responseHandlersHolder )
    {
        setOnce( channel, RESPONSE_HANDLERS_HOLDER, responseHandlersHolder );
    }

    private static <T> T get( Channel channel, AttributeKey<T> key )
    {
        return channel.attr( key ).get();
    }

    private static <T> void setOnce( Channel channel, AttributeKey<T> key, T value )
    {
        T existingValue = channel.attr( key ).setIfAbsent( value );
        if ( existingValue != null )
        {
            throw new IllegalStateException(
                    "Unable to set " + key.name() + " because it is already set to " + existingValue );
        }
    }
}
