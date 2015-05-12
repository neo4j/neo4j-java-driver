package org.neo4j.driver.internal.util;

import java.io.IOException;
import java.io.InputStream;

import org.neo4j.driver.exceptions.ClientException;

public class InputStreams
{
    /**
     * Reads from the input stream until the 'into' array is filled, or throws an exception.
     *
     * @param in
     * @param into
     */
    public static void readAll( InputStream in, byte[] into ) throws IOException
    {
        for(int idx = 0, read; idx<into.length; )
        {
            read = in.read( into, idx, into.length - idx );
            if(read == -1)
            {
                throw new ClientException(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected " + into.length + "bytes, " +
                        "recieved " + idx + ".");
            }
            else
            {
                idx += read;
            }
        }
    }
}
