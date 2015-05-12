package org.neo4j.driver.internal.util;

/**
 * Since {@link java.time.Clock} is only available in Java 8, use our own until we drop java 7 support.
 */
public interface Clock
{
    /** Current time, in milliseconds. */
    long millis();

    Clock SYSTEM = new Clock()
    {
        @Override
        public long millis()
        {
            return System.currentTimeMillis();
        }
    };
}
