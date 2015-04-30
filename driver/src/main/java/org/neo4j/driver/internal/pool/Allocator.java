package org.neo4j.driver.internal.pool;

import org.neo4j.Neo4j;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.util.Consumer;

public interface Allocator<Value>
{
    /**
     * Called when the pool needs a new value created. The 'release' handle given here will return the object to the
     * pool. How it gets invoked is up to the pooled object, but a suggested pattern is for the pooled object to
     * implement a 'close' method which calls the release handle.
     */
    Value create( Consumer<Value> release );

    /** Called when a value gets kicked out of the pool. */
    void onDispose( Value value );

    /** Called when a value gets acquired from the pool */
    void onAcquire( Value value );
}
