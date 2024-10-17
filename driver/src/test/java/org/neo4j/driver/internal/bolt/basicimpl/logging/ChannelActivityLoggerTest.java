/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;

class ChannelActivityLoggerTest {
    @Test
    void shouldReformatWhenChannelIsNull() {
        var activityLogger = new ChannelActivityLogger(null, NoopLoggingProvider.INSTANCE, getClass());

        var reformatted = activityLogger.reformat("Hello!");

        assertEquals("Hello!", reformatted);
    }

    @Test
    void shouldReformatWithChannelId() {
        var channel = new EmbeddedChannel();
        var activityLogger = new ChannelActivityLogger(channel, NoopLoggingProvider.INSTANCE, getClass());

        var reformatted = activityLogger.reformat("Hello!");

        assertEquals("[0x" + channel.id() + "][][] Hello!", reformatted);
    }

    @Test
    void shouldReformatWithChannelIdAndServerAddress() {
        var channel = new EmbeddedChannel();
        ChannelAttributes.setServerAddress(channel, new BoltServerAddress("somewhere", 1234));
        var activityLogger = new ChannelActivityLogger(channel, NoopLoggingProvider.INSTANCE, getClass());

        var reformatted = activityLogger.reformat("Hello!");

        assertEquals("[0x" + channel.id() + "][somewhere:1234][] Hello!", reformatted);
    }

    @Test
    void shouldReformatWithChannelIdAndConnectionId() {
        var channel = new EmbeddedChannel();
        ChannelAttributes.setConnectionId(channel, "bolt-12345");
        var activityLogger = new ChannelActivityLogger(channel, NoopLoggingProvider.INSTANCE, getClass());

        var reformatted = activityLogger.reformat("Hello!");

        assertEquals("[0x" + channel.id() + "][][bolt-12345] Hello!", reformatted);
    }
}
