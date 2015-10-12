package org.neo4j.driver;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.ParameterSupport;

import static java.lang.String.format;

/**
 * An executable statement, i.e. the statement's text and it's parameters.
 *
 * @see org.neo4j.driver.Session
 * @see org.neo4j.driver.Transaction
 * @see org.neo4j.driver.Result
 * @see org.neo4j.driver.Result#summarize()
 * @see org.neo4j.driver.ResultSummary
 */
public class Statement
{
    private final String text;
    private final Map<String, Value> parameters;

    public Statement( String text, Map<String, Value> parameters )
    {
        this.text = text;
        this.parameters = parameters == null || parameters.isEmpty()
            ? ParameterSupport.NO_PARAMETERS
            : Collections.unmodifiableMap( parameters );
    }

    public Statement( String text )
    {
        this( text, null );
    }

    /**
     * @return the statement's text
     */
    public String text()
    {
        return text;
    }

    /**
     * @return the statement's parameters
     */
    public Map<String, Value> parameters()
    {
        return parameters;
    }

    /**
     * @param newText the new statement's text
     * @return a new statement with updated text
     */
    public Statement withText( String newText )
    {
        return new Statement( newText, parameters );
    }

    /**
     * @param newParameters the new statement's parameters
     * @return a new statement with updated parameters
     */
    public Statement withParameters( Map<String, Value> newParameters )
    {
        return new Statement( text, newParameters );
    }

    /**
     * Create a new statement with new parameters derived by updating this'
     * statement's parameters using the given updates.
     *
     * Every update key that points to a null value will be removed from
     * the new statement's parameters. All other entries will just replace
     * any existing parameter in the new statement.
     *
     * @param updates describing how to update the parameters
     * @return a new statement with updated parameters
     */
    public Statement withUpdatedParameters( Map<String, Value> updates )
    {
        if ( updates == null || updates.isEmpty() )
        {
            return this;
        }
        else
        {
            Map<String, Value> newParameters = new HashMap<>( Math.max( parameters.size(), updates.size() ) );
            newParameters.putAll( parameters );
            for ( Map.Entry<String, Value> entry : updates.entrySet() )
            {
                Value value = entry.getValue();
                if ( value == null )
                {
                    newParameters.remove( entry.getKey() );
                }
                else
                {
                    newParameters.put( entry.getKey(), value );
                }
            }
            return withParameters( newParameters );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Statement statement = (Statement) o;
        return text.equals( statement.text ) && parameters.equals( statement.parameters );

    }

    @Override
    public int hashCode()
    {
        int result = text.hashCode();
        result = 31 * result + parameters.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "Statement{text='%s', parameters=%s}", text, parameters );
    }
}
