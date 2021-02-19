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
package org.neo4j.driver.internal.messaging;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setProtocolVersion;

class BoltProtocolTest
{
    @Test
    void shouldCreateProtocolForKnownVersions()
    {
        assertAll(
                () -> assertThat( BoltProtocol.forVersion( BoltProtocolV3.VERSION ), instanceOf( BoltProtocolV3.class ) )
        );
    }

    @Test
    void shouldThrowForUnknownVersion()
    {
        assertAll(
                () -> assertThrows( ClientException.class, () -> BoltProtocol.forVersion( new BoltProtocolVersion(  42, 0 ) ) ),
                () -> assertThrows( ClientException.class, () -> BoltProtocol.forVersion( new BoltProtocolVersion( 142, 0 ) ) ),
                () -> assertThrows( ClientException.class, () -> BoltProtocol.forVersion( new BoltProtocolVersion( -1, 0 ) ) )
        );
    }

    @Test
    void shouldThrowForChannelWithUnknownProtocolVersion()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        setProtocolVersion( channel, new BoltProtocolVersion( 42, 0 ) );

        assertThrows( ClientException.class, () -> BoltProtocol.forChannel( channel ) );
    }
}
