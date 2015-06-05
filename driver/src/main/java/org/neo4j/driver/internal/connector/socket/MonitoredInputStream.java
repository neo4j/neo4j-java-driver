/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
