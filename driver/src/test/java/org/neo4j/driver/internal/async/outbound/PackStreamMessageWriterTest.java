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
package org.neo4j.driver.internal.async.outbound;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.IOException;

import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1.MSG_DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1.MSG_PULL_ALL;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.packstream.PackStream.TINY_STRUCT;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufContains;

public class PackStreamMessageWriterTest
{
    @Test
    public void shouldWritePullAll() throws IOException
    {
        PackStreamMessageWriter writer = new PackStreamMessageWriter();
        ByteBuf buf = Unpooled.buffer();

        writer.write( PULL_ALL, buf );

        assertByteBufContains( buf, (short) 2, TINY_STRUCT, MSG_PULL_ALL );
    }

    @Test
    public void shouldWriteDiscardAll() throws IOException
    {
        PackStreamMessageWriter writer = new PackStreamMessageWriter();
        ByteBuf buf = Unpooled.buffer();

        writer.write( DISCARD_ALL, buf );

        assertByteBufContains( buf, (short) 2, TINY_STRUCT, MSG_DISCARD_ALL );
    }
}
