package org.neo4j.driver.internal.connector.socket;

import java.io.InputStream;
import java.io.OutputStream;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

public class SocketProtocolV1 implements SocketProtocol
{
    private final MessageFormat messageFormat;
    private final ChunkedInput input;
    private final ChunkedOutput output;
    private final Reader reader;
    private Writer writer;
    private static final int VERSION = 1;

    public SocketProtocolV1()
    {
        messageFormat = new PackStreamMessageFormatV1();

        this.output = new ChunkedOutput();
        this.input = new ChunkedInput();

        this.writer = new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook() );
        this.reader = new PackStreamMessageFormatV1.Reader( input );
    }

    @Override
    public MessageFormat messageFormat()
    {
        return messageFormat;
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
    public void outputStream( OutputStream out )
    {
        output.setOutputStream( out );
    }

    @Override
    public void inputStream( InputStream in )
    {
        input.setInputStream( in );
    }

    @Override
    public int version()
    {
        assert VERSION == messageFormat.version();
        return VERSION;
    }
}
