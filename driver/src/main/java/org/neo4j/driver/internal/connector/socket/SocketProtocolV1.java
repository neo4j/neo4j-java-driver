package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

public class SocketProtocolV1 implements SocketProtocol
{
    private final MessageFormat messageFormat;
    private final Reader reader;
    private final Writer writer;

    public SocketProtocolV1( SocketChannel channel ) throws IOException
    {
        messageFormat = new PackStreamMessageFormatV1();

        ChunkedOutput output = new ChunkedOutput( channel );
        ChunkedInput input = new ChunkedInput( channel );

        this.writer = new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook() );
        this.reader = new PackStreamMessageFormatV1.Reader( input, input.messageBoundaryHook() );
    }

    @Override
    public Reader reader()
    {
        return reader;
    }

    @Override
    public Writer writer()
    {
        return writer;
    }

    @Override
    public int version()
    {
        return messageFormat.version();
    }
}
