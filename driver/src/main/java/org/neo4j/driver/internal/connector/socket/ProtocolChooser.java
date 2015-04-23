package org.neo4j.driver.internal.connector.socket;

import java.nio.ByteBuffer;

import org.neo4j.driver.exceptions.ClientException;

public class ProtocolChooser
{
    // TODO change this silly hard-coded code
    public static byte[] supportedVersions()
    {
        return new byte[] { 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    }

    public static SocketProtocol chooseVersion( int version )
    {
        if ( version == 1 )
         {
            return new SocketProtocolV1();
        }
        else
        {
            throw new ClientException( "Cannot support selected protocol " + version );
        }
    }

    public static byte[] int2bytes( int... values )
    {
        ByteBuffer bytes = ByteBuffer.allocate( 4 * values.length );
        for ( int value : values )
        {
            bytes.putInt( value );
        }
        return bytes.array();
    }

    public static int bytes2Int( byte[] bytes )
    {
        assert bytes.length == 4;
        return ByteBuffer.wrap( bytes ).getInt();
    }
}
