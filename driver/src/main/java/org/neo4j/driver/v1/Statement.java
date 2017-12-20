/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1;

import java.util.Map;

import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Immutable;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.value;

/**
 * An executable statement, i.e. the statements' text and its parameters.
 *
 * @see Session
 * @see Transaction
 * @see StatementResult
 * @see StatementResult#consume()
 * @see ResultSummary
 * @since 1.0
 */
@Immutable
public class Statement
{
    private final String text;
    private final Value parameters;

    /**
     * Create a new statement.
     * @param text the statement text
     * @param parameters the statement parameters
     */
    public Statement( String text, Value parameters )
    {
        this.text = text;
        this.parameters = parameters == null ? Values.EmptyMap : parameters;
    }

    /**
     * Create a new statement.
     * @param text the statement text
     * @param parameters the statement parameters
     */
    public Statement( String text, Map<String, Object> parameters )
    {
        this( text, Values.value( parameters ) );
    }

    /**
     * Create a new statement.
     * @param text the statement text
     */
    public Statement( String text )
    {
        this( text, Values.EmptyMap );
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
    public Value parameters()
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
    public Statement withParameters( Value newParameters )
    {
        return new Statement( text, newParameters );
    }

    /**
     * @param newParameters the new statement's parameters
     * @return a new statement with updated parameters
     */
    public Statement withParameters( Map<String, Object> newParameters )
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
    public Statement withUpdatedParameters( Value updates )
    {
        if ( updates == null || updates.isEmpty() )
        {
            return this;
        }
        else
        {
            Map<String,Value> newParameters = newHashMapWithSize( Math.max( parameters.size(), updates.size() ) );
            newParameters.putAll( parameters.asMap( ofValue() ) );
            for ( Map.Entry<String, Value> entry : updates.asMap( ofValue() ).entrySet() )
            {
                Value value = entry.getValue();
                if ( value.isNull() )
                {
                    newParameters.remove( entry.getKey() );
                }
                else
                {
                    newParameters.put( entry.getKey(), value );
                }
            }
            return withParameters( value(newParameters) );
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
