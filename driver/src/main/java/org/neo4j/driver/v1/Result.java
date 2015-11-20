/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * The result of running a statement, a stream of records. The result interface can be used to iterate over all the
 * records in the stream, and for each record to access the fields within it using the {@link #get(int) get} methods.
 *
 * Results are valid until the next statement is run or until the end of the current transaction, whichever comes
 * first.
 *
 * To keep a result around while further statements are run, or to use a result outside the scope of the current
 * transaction, see {@link #retain()}.
 */
public interface Result
{
    /**
     * Retrieve and store the entire result stream. This can be used if you want to
     * iterate over the stream multiple times or to store the whole result for later use.
     *
     * This cannot be used if you have already started iterating through the stream using {@link #next()}.
     *
     * @return {@link ReusableResult}
     */
    ReusableResult retain();

    /**
     * Move to the next record in the result.
     *
     * @return true if there was another record, false if the stream is exhausted.
     */
    boolean next();

    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     *
     * @param fieldIndex the field index into the current record
     * @return the value in the specified field
     */
    Value get( int fieldIndex );

    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     * If no value could be found in the specified filed, null will be returned.
     *
     * @param fieldName the field to retrieve the value from
     * @return the value in the specified field or null if no value could be found in the specified filed
     */
    Value get( String fieldName );

    /**
     * Get an ordered sequence of the field names in this result.
     *
     * @return field names
     */
    Iterable<String> fieldNames();

    /**
     * Retrieve the first field of the next record in the stream, and close the stream.
     *
     * This is a utility for the common case of statements that are expected to yield a single output value.
     *
     * <pre>
     * {@code
     * Record record = statement.run( "MATCH (n:User {uid:..}) RETURN n.name" ).single();
     * }
     * </pre>
     *
     * @return a single record from the stream
     * @throws ClientException if the stream is empty
     */
    Record single();

    /**
     * Summarize the result
     *
     * Any remaining (unprocessed) result records will be consumed.
     *
     * <pre class="doctest:ResultDocIT#summarizeUsage">
     * {@code
     * ResultSummary summary = session.run( "PROFILE MATCH (n:User {id: 12345}) RETURN n" ).summarize();
     * }
     * </pre>
     *
     * @return a summary for the whole query
     */
    ResultSummary summarize();
}
