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

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.net.BoltServerAddress;

import static io.netty.util.AttributeKey.newInstance;

public final class ChannelAttributes
{
    private static final AttributeKey<BoltServerAddress> ADDRESS = newInstance( "address" );
    private static final AttributeKey<Long> CREATION_TIMESTAMP = newInstance( "creationTimestamp" );
    private static final AttributeKey<Long> LAST_USED_TIMESTAMP = newInstance( "lastUsedTimestamp" );
    private static final AttributeKey<InboundMessageDispatcher> MESSAGE_DISPATCHER = newInstance( "messageDispatcher" );
    private static final AttributeKey<String> SERVER_VERSION = newInstance( "serverVersion" );

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

    public static Long lastUsedTimestamp( Channel channel )
    {
        return get( channel, LAST_USED_TIMESTAMP );
    }

    public static void setLastUsedTimestamp( Channel channel, long lastUsedTimestamp )
    {
        set( channel, LAST_USED_TIMESTAMP, lastUsedTimestamp );
    }

    public static InboundMessageDispatcher messageDispatcher( Channel channel )
    {
        return get( channel, MESSAGE_DISPATCHER );
    }

    public static void setMessageDispatcher( Channel channel, InboundMessageDispatcher messageDispatcher )
    {
        setOnce( channel, MESSAGE_DISPATCHER, messageDispatcher );
    }

    public static String serverVersion( Channel channel )
    {
        return get( channel, SERVER_VERSION );
    }

    public static void setServerVersion( Channel channel, String serverVersion )
    {
        setOnce( channel, SERVER_VERSION, serverVersion );
    }

    private static <T> T get( Channel channel, AttributeKey<T> key )
    {
        return channel.attr( key ).get();
    }

    private static <T> void set( Channel channel, AttributeKey<T> key, T value )
    {
        channel.attr( key ).set( value );
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
