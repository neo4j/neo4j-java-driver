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
package org.neo4j.driver.internal.net;

import java.nio.channels.ByteChannel;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

public class SocketProtocolV1 implements SocketProtocol
{
    private final MessageFormat messageFormat;
    private final Reader reader;
    private final Writer writer;

    public static SocketProtocol create( ByteChannel channel )
    {
        /*by default the byte array support is enabled*/
        return new SocketProtocolV1( channel, true );
    }

    public static SocketProtocol createWithoutByteArraySupport( ByteChannel channel )
    {
        return new SocketProtocolV1( channel, false );
    }

    private SocketProtocolV1( ByteChannel channel, boolean byteArraySupportEnabled )
    {
        messageFormat = new PackStreamMessageFormatV1();
        this.writer = messageFormat.newWriter( new ChunkedOutput( channel ), byteArraySupportEnabled );
        this.reader = messageFormat.newReader( new BufferingChunkedInput( channel ) );
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
