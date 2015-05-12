package org.neo4j.driver.internal.spi;


import java.net.URI;

public interface ConnectionPool extends AutoCloseable
{
    /**
     * Acquire a connection - if a live connection exists in the pool, it will be used, otherwise a new connection
     * is created with an applicable {@link Connector}.
     */
    Connection acquire( URI sessionURI );
}
