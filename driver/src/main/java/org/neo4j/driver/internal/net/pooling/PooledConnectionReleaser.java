package org.neo4j.driver.internal.net.pooling;

import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.spi.PooledConnection;

/**
 * The responsibility of the {@link PooledConnectionReleaser} is to release valid connections
 * back to the connections queue.
 */
public interface PooledConnectionReleaser
{
    void accept( PooledConnection pooledConnection ) throws InvalidOperationException;
}
