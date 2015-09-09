package org.neo4j.driver;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import org.neo4j.driver.util.TestSession;

import static org.junit.Assert.assertEquals;

@RunWith( DocTestRunner.class )
public class TransactionDocIT
{
    @Rule
    public TestSession session = new TestSession();

    /** @see Transaction */
    public void classDoc( DocSnippet snippet )
    {
        // Given
        snippet.set( "session", session );

        // When I run the snippet
        snippet.run();

        // Then a node should've been created
        assertEquals( 1, session.run( "MATCH (n) RETURN count(n)" ).single().get( "count(n)" ).javaInteger() );
    }

    /** @see Transaction#failure()  */
    public void failure( DocSnippet snippet )
    {
        // Given
        snippet.set( "session", session );

        // When I run the snippet
        snippet.run();

        // Then a node should've been created
        assertEquals( 0, session.run( "MATCH (n) RETURN count(n)" ).single().get( "count(n)" ).javaInteger() );
    }
}
