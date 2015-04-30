package org.neo4j.driver.internal.pool;

/**
 * Validates connections - determining if they are ok to keep in the pool, or if they should be disposed of.
 */
public class PooledConnectionValidator implements ValidationStrategy<PooledConnection>
{
    private final long maxIdleMillis;

    public PooledConnectionValidator( long maxIdleTimeMillis )
    {
        this.maxIdleMillis = maxIdleTimeMillis;
    }

    @Override
    public boolean isValid( PooledConnection value, long idleTime )
    {
        return idleTime < maxIdleMillis && !value.hasUnrecoverableErrors();
    }
}
