package org.neo4j.driver.internal.connector.socket;

import java.io.InputStream;
import java.io.OutputStream;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;

public interface SocketProtocol
{
    Reader reader();

    Writer writer();

    void outputStream( OutputStream out );

    void inputStream( InputStream in );

    int version();
}
