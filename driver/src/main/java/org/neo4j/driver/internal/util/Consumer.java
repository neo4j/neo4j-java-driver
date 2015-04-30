package org.neo4j.driver.internal.util;

public interface Consumer<T>
{
    void accept(T t);
}
