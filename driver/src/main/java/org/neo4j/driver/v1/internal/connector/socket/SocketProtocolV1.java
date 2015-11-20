/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.connector.socket;

import java.io.IOException;
import java.nio.channels.ByteChannel;

import org.neo4j.driver.v1.internal.messaging.MessageFormat;
import org.neo4j.driver.v1.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.v1.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.v1.internal.messaging.PackStreamMessageFormatV1;

public class SocketProtocolV1 implements SocketProtocol
{
    private final MessageFormat messageFormat;
    private final Reader reader;
    private final Writer writer;

    public SocketProtocolV1( ByteChannel channel ) throws IOException
    {
        messageFormat = new PackStreamMessageFormatV1();

        ChunkedOutput output = new ChunkedOutput( channel );
        ChunkedInput input = new ChunkedInput( channel );

        this.writer = new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook() );
        this.reader = new PackStreamMessageFormatV1.Reader( input, input.messageBoundaryHook() );
    }

    @Override
    public Reader reader()
    {
        return reader;
    }

    @Override
    public Writer writer()
    {
        return writer;
    }

    @Override
    public int version()
    {
        return messageFormat.version();
    }
}
