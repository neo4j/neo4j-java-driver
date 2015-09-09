package org.neo4j.driver;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import org.neo4j.driver.util.TestNeo4j;

import static org.junit.Assert.assertNotNull;

@RunWith( DocTestRunner.class )
public class GraphDatabaseDocIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    /** @see GraphDatabase */
    public void simpleExample( DocSnippet codeSnippet )
    {
        // When I run the snippet
        codeSnippet.run();

        // Then the driver should have been created
        assertNotNull( codeSnippet.get( "driver" ) );
    }
}
