package org.neo4j.driver.v1.exceptions;

/**
 * A signal that the contract for client-server communication has broken down.
 * The user should contact support and cannot resolve this his or herself.
 */
public class ProtocolException extends RuntimeException
{
    private static String CODE = "Protocol violation: ";
    public ProtocolException( String message )
    {
        super( CODE + message );
    }

    public ProtocolException( String message, Throwable e )
    {
        super( CODE + message, e );
    }
}
