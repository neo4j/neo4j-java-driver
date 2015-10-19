package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.util.TestNeo4j;

import static org.junit.Assert.assertFalse;

public class SessionIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Test
    public void shouldKnowSessionIsClosed() throws Throwable
    {
        // Given
        Driver driver = GraphDatabase.driver( neo4j.address() );
        Session session = driver.session();

        // When
        session.close();

        // Then
        assertFalse( session.isOpen() );
    }
}
