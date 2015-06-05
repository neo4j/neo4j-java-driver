package org.neo4j.driver.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class RecordingByteChannel implements WritableByteChannel, ReadableByteChannel
{
    private final ByteBuffer buffer = ByteBuffer.allocate( 16 * 1024 );
    private int writePosition = 0;
    private int readPosition = 0;
    private boolean eof;

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        buffer.position( writePosition );
        int originalPosition = writePosition;

        buffer.put( src );

        writePosition = buffer.position();
        return writePosition - originalPosition;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( readPosition == writePosition )
        {
            return eof ? -1 : 0;
        }
        buffer.position( readPosition );
        int originalPosition = readPosition;
        int originalLimit = buffer.limit();

        buffer.limit( Math.min( buffer.position() + (dst.limit() - dst.position()), writePosition ) );
        dst.put( buffer );

        readPosition = buffer.position();
        buffer.limit( originalLimit );
        return readPosition - originalPosition;
    }

    public byte[] getBytes()
    {
        byte[] bytes = new byte[buffer.position()];
        buffer.position( 0 );
        buffer.get( bytes );
        return bytes;
    }

    /**
     * Mark this buffer as ended. Once whatever is currently unread in it is consumed,
     * it will start yielding -1 responses.
     */
    public void eof()
    {
        eof = true;
    }
}
