/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Resource;
import org.neo4j.driver.v1.value.Values;


/**
 * The result of running a statement, a stream of {@link Record records}.
 *
 * The result stream can be used to iterate over all the records in the stream and provide access
 * to their content.
 *
 * Results are valid until the next statement is run or until the end of the current transaction,
 * whichever comes first. To keep a result around while further statements are run, or to use a result outside the scope
 * of the current transaction, see {@link #list()}.
 */
public interface ResultStream extends Iterator<Record>, Resource
{
    /**
     * Retrieve the keys of the records this result contains.
     *
     * @return all keys
     */
    List<String> keys();

    /**
     * Test if there is another record remaining in this result
     * @return true if {@link #next()} will return another record
     */
    @Override boolean hasNext();

    /**
     * Retrieve the next {@link Record} in this result.
     * @return the next record
     */
    @Override Record next();

    /**
     * Skip ahead, as if calling {@link #next()} multiple times.
     *
     * @throws ClientException if records is negative
     * @param records amount of records to be skipped
     * @return the actual number of records successfully skipped
     */
    long skip( long records );

    /**
     * Limit this result to return no more than the given number of records after the current record.
     * As soon as the described amount of records have been returned, all further records are discarded.
     * Calling limit again before the described amount of records have been returned, overwrites the previous limit.
     *
     * @throws ClientException if records is negative
     * @param records the maximum number of records to return from future calls to {@link #next()}
     * @return the actual position of the last record to be returned
     */
    long limit( long records );

    /**
     * Return the first record in the stream. Fail with an exception if the stream is empty
     * or if this cursor has already been used to move past the first record.
     *
     * @return the first record in the stream
     * @throws NoSuchRecordException if there is no first record or the cursor has been used already
     *
     */
    Record first() throws NoSuchRecordException;

    /**
     * Return the first record in the result, failing if there is not exactly
     * one record, or if this result has already been used to move past the first record.
     *
     * @return the first and only record in the stream
     * @throws NoSuchRecordException if there is not exactly one record in the stream, or if the cursor has been used already
     */
    Record single() throws NoSuchRecordException;

    /**
     * Investigate the next upcoming record without moving forward in the result.
     *
     * @return the next record, or null if there is no next record
     */
    Record peek();

    /**
     * Retrieve and store the entire result stream.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     *
     * Calling this method exhausts the result.
     *
     * @throws ClientException if the result has already been used
     * @return list of all immutable records
     */
    List<Record> list();

    /**
     * Retrieve and store a projection of the entire result.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     *
     * Calling this method exhausts the result.
     *
     * @throws ClientException if the result has already been used
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the type of result list elements
     * @return list of all mapped immutable records
     */
    <T> List<T> list( Function<Record, T> mapFunction );

    /**
     * Summarize the result.
     *
     * Calling this method exhausts the result.
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