package org.neo4j.driver.v1.exceptions;

/**
 * Thrown if the remote server cannot be verified as Neo4j.
 */
public class UntrustedServerException extends RuntimeException
{
    public UntrustedServerException(String message)
    {
        super(message);
    }
}
