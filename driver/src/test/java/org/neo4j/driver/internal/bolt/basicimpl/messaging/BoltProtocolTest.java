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
package org.neo4j.driver.internal.bolt.basicimpl.messaging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setProtocolVersion;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v43.BoltProtocolV43;

class BoltProtocolTest {
    @Test
    void shouldCreateProtocolForKnownVersions() {
        assertAll(
                () -> assertThat(BoltProtocol.forVersion(BoltProtocolV3.VERSION), instanceOf(BoltProtocolV3.class)),
                () -> assertThat(BoltProtocol.forVersion(BoltProtocolV4.VERSION), instanceOf(BoltProtocolV4.class)),
                () -> assertThat(BoltProtocol.forVersion(BoltProtocolV41.VERSION), instanceOf(BoltProtocolV41.class)),
                () -> assertThat(BoltProtocol.forVersion(BoltProtocolV42.VERSION), instanceOf(BoltProtocolV42.class)),
                () -> assertThat(BoltProtocol.forVersion(BoltProtocolV43.VERSION), instanceOf(BoltProtocolV43.class)));
    }

    @Test
    void shouldThrowForUnknownVersion() {
        assertAll(
                () -> assertThrows(
                        ClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(42, 0))),
                () -> assertThrows(
                        ClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(142, 0))),
                () -> assertThrows(
                        ClientException.class, () -> BoltProtocol.forVersion(new BoltProtocolVersion(-1, 0))));
    }

    @Test
    void shouldThrowForChannelWithUnknownProtocolVersion() {
        var channel = new EmbeddedChannel();
        setProtocolVersion(channel, new BoltProtocolVersion(42, 0));

        assertThrows(ClientException.class, () -> BoltProtocol.forChannel(channel));
    }
}
