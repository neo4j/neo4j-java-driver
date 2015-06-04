package org.neo4j.driver.internal.messaging;

import java.io.IOException;

import static java.lang.String.format;

/**
 * INITIALIZE request message
 * <p>
 * Sent by clients to initialize a new connection. Must be sent as the very first message after protocol negotiation.
 */
public class InitializeMessage implements Message
{
    private final String clientNameAndVersion;

    public InitializeMessage( String clientNameAndVersion )
    {
        this.clientNameAndVersion = clientNameAndVersion;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleInitializeMessage( clientNameAndVersion );
    }

    @Override
    public String toString()
    {
        return format( "[INITIALIZE \"%s\"]", clientNameAndVersion );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InitializeMessage that = (InitializeMessage) o;

        return !(clientNameAndVersion != null ? !clientNameAndVersion.equals( that.clientNameAndVersion )
                                              : that.clientNameAndVersion != null);

    }

    @Override
    public int hashCode()
    {
        return clientNameAndVersion != null ? clientNameAndVersion.hashCode() : 0;
    }
}
