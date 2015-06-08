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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.util.BytePrinter;

// TODO: These should be modified to be ReadableByteChannel, rather than InputStream
public class MonitoredInputStream extends InputStream
{
    private final InputStream realInput;
    private final Logger logger;

    public MonitoredInputStream( InputStream inputStream, Logger logger )
    {
        this.realInput = inputStream;
        this.logger = logger;
    }

    @Override
    public int read() throws IOException
    {
        int read = realInput.read();
        logger.log( Level.FINEST, "Input:\n" + BytePrinter.hex( (byte) read ) );
        return read;
    }

    @Override
    public int read( byte b[] ) throws IOException
    {
        int read = realInput.read( b );
        logger.log( Level.FINEST, "Input:\n" + BytePrinter.hex( b ) );
        return read;
    }

    @Override
    public int read( byte b[], int off, int len ) throws IOException
    {
        int read = realInput.read( b, off, len );
        logger.log( Level.FINEST, "Input:\n" + BytePrinter.hex( ByteBuffer.wrap( b ), off, len ) );
        return read;
    }

    @Override
    public long skip( long n ) throws IOException
    {
        return realInput.skip( n );
    }

    @Override
    public int available() throws IOException
    {
        return realInput.available();
    }

    @Override
    public void close() throws IOException
    {
        realInput.close();
    }

    @Override
    public synchronized void mark( int readlimit )
    {
        realInput.mark( readlimit );
    }

    @Override
    public synchronized void reset() throws IOException
    {
        realInput.reset();
    }

    @Override
    public boolean markSupported()
    {
        return realInput.markSupported();
    }
}
