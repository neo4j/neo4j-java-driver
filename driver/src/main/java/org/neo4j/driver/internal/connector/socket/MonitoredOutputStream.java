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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.util.BytePrinter;

// TODO: These should be modified to be WritableByteChannel, rather than OutputStream
public class MonitoredOutputStream extends OutputStream
{
    private final OutputStream realOut;
    private final Logger logger;

    public MonitoredOutputStream( OutputStream outputStream, Logger logger )
    {
        this.realOut = outputStream;
        this.logger = logger;
    }

    @Override
    public void write( int b ) throws IOException
    {
        realOut.write( b );
        logger.log( Level.FINEST, "Output:\n" + BytePrinter.hex( (byte) b ) );
    }

    @Override
    public void write( byte b[], int off, int len ) throws IOException
    {
        realOut.write( b, off, len );
        logger.log( Level.FINEST, "Output:\n" + BytePrinter.hex( ByteBuffer.wrap( b ), off, len ) );
    }

    @Override
    public void write( byte b[] ) throws IOException
    {
        realOut.write( b );
        logger.log( Level.FINEST, "Output:\n" + BytePrinter.hex( b ) );
    }

    @Override
    public void flush() throws IOException
    {
        realOut.flush();
    }

    @Override
    public void close() throws IOException
    {
        realOut.close();
    }
}
