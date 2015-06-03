package org.neo4j.driver.internal.connector.socket;

import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;

public interface SocketProtocol
{
    Reader reader();

    Writer writer();

    int version();
}
