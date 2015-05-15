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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.spi.Logging;
import org.neo4j.driver.internal.util.BytePrinter;

public class MonitoredOutputStream extends OutputStream
{
    OutputStream realOut;
    Logger logger;

    public MonitoredOutputStream( OutputStream outputStream, Logging logging )
    {
        this.realOut = outputStream;
        if( logging != null )
        {
            this.logger = logging.getLogging( getClass().getName() );
        }
        else
        {
            this.logger = new DevNullLogger();
        }
    }

    @Override
    public void write( int b ) throws IOException
    {
        realOut.write( b );
        logger.debug( "Output:\n" + BytePrinter.hex( (byte) b ) );
    }

    @Override
    public void write( byte b[], int off, int len ) throws IOException
    {
        realOut.write( b, off, len );
        logger.debug( "Output:\n" + BytePrinter.hex( ByteBuffer.wrap( b ), off, len ) );
    }

    @Override
    public void write( byte b[] ) throws IOException
    {
        realOut.write( b );
        logger.debug( "Output:\n" + BytePrinter.hex( b ) );
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
