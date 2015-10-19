package org.neo4j.driver;

import java.util.Map;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.ParameterSupport.NO_PARAMETERS;

public class StatementTest
{
    @Test
    public void shouldConstructStatementWithParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text, NO_PARAMETERS );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }

    @Test
    public void shouldConstructStatementWithNoParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }

    @Test
    public void shouldConstructStatementWithNullParameters()
    {
        // given
        String text = "MATCH (n) RETURN n";

        // when
        Statement statement = new Statement( text, null );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }


    @Test
    public void shouldUpdateStatementText()
    {
        // when
        Statement statement =
                new Statement( "MATCH (n) RETURN n" )
                .withText( "BOO" );

        // then
        assertThat( statement.text(), equalTo( "BOO" ) );
        assertThat( statement.parameters(), equalTo( NO_PARAMETERS ) );
    }


    @Test
    public void shouldReplaceStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Map<String, Value> initialParameters = parameters( "a", 1, "b", 2 );
        Statement statement = new Statement( "MATCH (n) RETURN n" ).withParameters( initialParameters );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( initialParameters ) );
    }


    @Test
    public void shouldUpdateStatementParameters()
    {
        // when
        String text = "MATCH (n) RETURN n";
        Map<String, Value> initialParameters = parameters( "a", 1, "b", 2, "c", 3 );
        Statement statement =
                new Statement( "MATCH (n) RETURN n", initialParameters )
                .withUpdatedParameters( parameters( "a", 0, "b", null ) );

        // then
        assertThat( statement.text(), equalTo( text ) );
        assertThat( statement.parameters(), equalTo( parameters( "a", 0, "c", 3 ) ) );
    }
}
