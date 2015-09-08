package org.neo4j.driver;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.properties;

@RunWith( DocTestRunner.class )
public class ValueDocIT
{
    private final Value exampleValue = Values.value(properties( "users", asList( properties( "name", "Anders" ), properties( "name", "John" ) ) ));

    public void classDocTreeExample( DocSnippet snippet )
    {
        // given
        snippet.set( "value", exampleValue );

        // when
        snippet.run();

        // then
        assertThat( snippet.get("username"), equalTo( (Object)"John" ));
    }

    public void classDocIterationExample( DocSnippet snippet )
    {
        // given
        snippet.addImport( LinkedList.class );
        snippet.addImport( List.class );
        snippet.set( "value", exampleValue );

        // when
        snippet.run();

        // then
        assertThat( snippet.get("names"), equalTo( (Object)asList("Anders","John") ));
    }
}
