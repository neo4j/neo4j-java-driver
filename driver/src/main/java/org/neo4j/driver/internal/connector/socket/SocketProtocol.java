package org.neo4j.driver.internal.connector.socket;

import java.io.InputStream;
import java.io.OutputStream;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;

public interface SocketProtocol
{
    public MessageFormat messageFormat();

    public Reader reader();

    public Writer writer();

    public void outputStream( OutputStream out );

    public void inputStream( InputStream in );

    public int version();
}
