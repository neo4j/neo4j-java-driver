package org.neo4j.driver.internal.exceptions;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

/**
 * This exception represents an exception that is passed from the server.
 * When using bolt driver with a server, the server might return a failure message indicating something is wrong to
 * run a statement. The failure message could be mapped into a {@link org.neo4j.driver.v1.exceptions.Neo4jException}.
 *
 * This {@link ServerNeo4jException} wrap around the {@link org.neo4j.driver.v1.exceptions.Neo4jException} to make it
 * a checked exception so that we could keep track of it when it is passed internally. Then when we surface this
 * exception to the user, we will map it back to {@link org.neo4j.driver.v1.exceptions.Neo4jException}.
 */
public class ServerNeo4jException extends InternalException
{
    private final Neo4jException error;

    public ServerNeo4jException( String code, String message )
    {
        String[] parts = code.split( "\\." );
        String classification = parts[1];
        switch ( classification )
        {
        case "ClientError":
            error = new ClientException( code, message );
            break;
        case "TransientError":
            error = new TransientException( code, message );
            break;
        default:
            error = new DatabaseException( code, message );
            break;
        }
    }

    public ServerNeo4jException( Neo4jException error )
    {
        this.error = error;
    }

    @Override
    public Neo4jException publicException()
    {
        return error;
    }

    public boolean isProtocolViolationError()
    {
        return error != null && error.code().startsWith( "Neo.ClientError.Request" );
    }

    private boolean isDatabaseError()
    {
        return error != null && error instanceof DatabaseException;
    }

    @Override
    public boolean isUnrecoverableError()
    {
        return isDatabaseError() || isProtocolViolationError();
    }
}
