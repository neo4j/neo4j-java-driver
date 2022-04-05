/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver;

import java.util.Map;

import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.Immutable;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.value;

/**
 * The components of a Cypher query, containing the query text and parameter map.
 *
 * @see Session
 * @see Transaction
 * @see Result
 * @see Result#consume()
 * @see ResultSummary
 * @since 1.0
 */
@Immutable
public class Query
{
    private final String text;
    private final Value parameters;

    /**
     * Create a new query.
     * @param text the query text
     * @param parameters the parameter map
     */
    public Query(String text, Value parameters )
    {
        this.text = validateQueryText( text );
        if( parameters == null )
        {
            this.parameters = Values.EmptyMap;
        }
        else if ( parameters instanceof MapValue )
        {
            this.parameters = parameters;
        }
        else
        {
            throw new IllegalArgumentException( "The parameters should be provided as Map type. Unsupported parameters type: " + parameters.type().name() );
        }
    }

    /**
     * Create a new query.
     * @param text the query text
     * @param parameters the parameter map
     */
    public Query(String text, Map<String, Object> parameters )
    {
        this( text, Values.value( parameters ) );
    }

    /**
     * Create a new query.
     * @param text the query text
     */
    public Query(String text )
    {
        this( text, Values.EmptyMap );
    }

    /**
     * @return the query text
     */
    public String text()
    {
        return text;
    }

    /**
     * @return the parameter map
     */
    public Value parameters()
    {
        return parameters;
    }

    /**
     * @param newText the new query text
     * @return a new Query object with updated text
     */
    public Query withText(String newText )
    {
        return new Query( newText, parameters );
    }

    /**
     * @param newParameters the new parameter map
     * @return a new Query object with updated parameters
     */
    public Query withParameters(Value newParameters )
    {
        return new Query( text, newParameters );
    }

    /**
     * @param newParameters the new parameter map
     * @return a new Query object with updated parameters
     */
    public Query withParameters(Map<String, Object> newParameters )
    {
        return new Query( text, newParameters );
    }

    /**
     * Create a new query with new parameters derived by updating this'
     * query's parameters using the given updates.
     *
     * Every update key that points to a null value will be removed from
     * the new query's parameters. All other entries will just replace
     * any existing parameter in the new query.
     *
     * @param updates describing how to update the parameters
     * @return a new query with updated parameters
     */
    public Query withUpdatedParameters(Value updates )
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

        Query query = (Query) o;
        return text.equals( query.text ) && parameters.equals( query.parameters );

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
        return format( "Query{text='%s', parameters=%s}", text, parameters );
    }

    private static String validateQueryText(String query )
    {
        checkArgument( query != null, "Cypher query text should not be null" );
        checkArgument( !query.isEmpty(), "Cypher query text should not be an empty string" );
        return query;
    }
}
