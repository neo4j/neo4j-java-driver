package org.neo4j.driver;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.util.TestNeo4jSession;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

@RunWith( DocTestRunner.class )
public class DriverDocIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    /** @see Driver */
    public void exampleUsage( DocSnippet snippet )
    {
        // given
        snippet.addImport( List.class );
        snippet.addImport( LinkedList.class );

        // when
        snippet.run();

        // then it should've created a bunch of data
        assertEquals( 3, session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).javaInteger() );
        assertThat( (List<String>)snippet.get( "names" ), equalTo( asList("Bob", "Alice", "Tina")) );
    }
}
