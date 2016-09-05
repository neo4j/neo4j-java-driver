package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.ServerVersion;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import static org.neo4j.driver.v1.util.ServerVersion.v3_1_0;

public class BookmarkIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Before
    public void assumeBookmarkSupport()
    {
        System.out.println(session.server());
        assumeTrue( ServerVersion.version( session.server() ).greaterThanOrEqual( v3_1_0 ) );
    }

    @Test
    public void shouldReceiveBookmarkOnSuccessfulCommit() throws Throwable
    {
        // Given
        assertNull( session.lastBookmark() );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (a:Person)" );
            tx.success();
        }

        // Then
        assertNotNull( session.lastBookmark() );
    }
}
