package org.neo4j.driver.internal.pool;

public interface ValidationStrategy<T>
{
    boolean isValid( T value, long idleTime );
}
