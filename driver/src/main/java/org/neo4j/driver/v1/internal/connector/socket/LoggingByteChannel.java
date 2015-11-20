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
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import org.neo4j.driver.v1.internal.spi.Logger;
import org.neo4j.driver.v1.internal.util.BytePrinter;

/**
 * Basically it is a wrapper to a {@link ByteChannel} with logging enabled to record bytes sent and received over the
 * channel.
 */
public class LoggingByteChannel implements ByteChannel
{
    private final ByteChannel delegate;
    private final Logger logger;


    public LoggingByteChannel( ByteChannel delegate, Logger logger ) throws IOException
    {
        this.delegate = delegate;
        this.logger = logger;
    }

    @Override
    public int write( ByteBuffer buf ) throws IOException
    {
        int offset = buf.position();
        int length = delegate.write( buf );
        logger.trace( "C: " + BytePrinter.hexInOneLine( buf, offset, length ) );
        return length;
    }

    @Override
    public int read( ByteBuffer buf ) throws IOException
    {
        int offset = buf.position();
        int length = delegate.read( buf );
        logger.trace( "S: " + BytePrinter.hexInOneLine( buf, offset, length ) );
        return length;
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    public void close() throws IOException
    {
        delegate.close();
    }
}
