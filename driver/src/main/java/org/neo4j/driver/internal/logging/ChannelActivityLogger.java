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
package org.neo4j.driver.internal.logging;

import io.netty.channel.Channel;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Format.valueOrEmpty;

public class ChannelActivityLogger extends ReformattedLogger
{
    private final Channel channel;
    private final String localChannelId;

    private String dbConnectionId;
    private String serverAddress;

    public ChannelActivityLogger( Channel channel, Logging logging, Class<?> owner )
    {
        this( channel, logging.getLog( owner.getSimpleName() ) );
    }

    private ChannelActivityLogger( Channel channel, Logger delegate )
    {
        super( delegate );
        this.channel = channel;
        this.localChannelId = channel != null ? channel.id().toString() : null;
    }

    @Override
    protected String reformat( String message )
    {
        if ( channel == null )
        {
            return message;
        }

        String dbConnectionId = getDbConnectionId();
        String serverAddress = getServerAddress();

        return format( "[0x%s][%s][%s] %s", localChannelId, valueOrEmpty( serverAddress ), valueOrEmpty( dbConnectionId ), message );
    }

    private String getDbConnectionId()
    {
        if ( dbConnectionId == null )
        {
            dbConnectionId = ChannelAttributes.connectionId( channel );
        }
        return dbConnectionId;
    }

    private String getServerAddress()
    {

        if ( serverAddress == null )
        {
            BoltServerAddress serverAddress = ChannelAttributes.serverAddress( channel );
            this.serverAddress = serverAddress != null ? serverAddress.toString() : null;
        }

        return serverAddress;
    }
}
