package org.neo4j.driver;

/**
 * This represents a statement runner that is auto-closeable. The Neo4j Driver
 * doesn't use any checked exceptions in {@link #close()}.
 *
 * This interface exists separately from {@link StatementRunner} so that an
 * API consumer can control which capabilities are provided to one of its
 * own components (i.e. just running statements vs. running statements and
 * being able to close to runner).
 */
public interface CloseableStatementRunner extends StatementRunner, AutoCloseable
{
    /**
     * Close the underlying statement runner. Any further attempts to run
     * statements via this runner will fail. Also after calling close, {@link StatementRunner#isOpen()}
     * returns false.
     *
     * This method is NOT idempotent; calling it twice will throw a {@link org.neo4j.driver.exceptions.ClientException}.
     *
     * @throws org.neo4j.driver.exceptions.ClientException
     */
    @Override
    void close();
}
