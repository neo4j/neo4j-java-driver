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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.Logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChannelActivityLoggerTest
{
    @Test
    void shouldReformatWhenChannelIsNull()
    {
        ChannelActivityLogger activityLogger = new ChannelActivityLogger( null, Logging.none(), getClass() );

        String reformatted = activityLogger.reformat( "Hello!" );

        assertEquals( "Hello!", reformatted );
    }

    @Test
    void shouldReformatWithChannelId()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelActivityLogger activityLogger = new ChannelActivityLogger( channel, Logging.none(), getClass() );

        String reformatted = activityLogger.reformat( "Hello!" );

        assertEquals( "[0x" + channel.id() + "][][] Hello!", reformatted );
    }

    @Test
    void shouldReformatWithChannelIdAndServerAddress()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelAttributes.setServerAddress( channel, new BoltServerAddress( "somewhere", 1234 ) );
        ChannelActivityLogger activityLogger = new ChannelActivityLogger( channel, Logging.none(), getClass() );

        String reformatted = activityLogger.reformat( "Hello!" );

        assertEquals( "[0x" + channel.id() + "][somewhere:1234][] Hello!", reformatted );
    }

    @Test
    void shouldReformatWithChannelIdAndConnectionId()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelAttributes.setConnectionId( channel, "bolt-12345" );
        ChannelActivityLogger activityLogger = new ChannelActivityLogger( channel, Logging.none(), getClass() );

        String reformatted = activityLogger.reformat( "Hello!" );

        assertEquals( "[0x" + channel.id() + "][][bolt-12345] Hello!", reformatted );
    }
}
