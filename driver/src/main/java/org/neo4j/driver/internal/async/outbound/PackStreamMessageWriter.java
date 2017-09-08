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

import java.io.IOException;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

// todo: this class should take MessageFormat as input and create MessageFormat.Writer with ChunkAwareByteBufOutput
public class PackStreamMessageWriter implements OutboundMessageWriter
{
    private final ChunkAwareByteBufOutput output;
    private final MessageFormat.Writer writer;

    public PackStreamMessageWriter()
    {
        this.output = new ChunkAwareByteBufOutput();
        this.writer = new PackStreamMessageFormatV1.Writer( output, new Runnable()
        {
            @Override
            public void run()
            {
            }
        }, true );
    }

    @Override
    public void write( Message message, ByteBuf buf ) throws IOException
    {
        output.start( buf );
        writer.write( message );
        output.stop();
    }
}
