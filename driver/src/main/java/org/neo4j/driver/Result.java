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

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.Resource;


/**
 * The result of running a Cypher query, conceptually a stream of {@link Record records}.
 *
 * The standard way of navigating through the result returned by the database is to
 * {@link #next() iterate} over it.
 *
 * Results are valid until the next query is run or until the end of the current transaction,
 * whichever comes first. To keep a result around while further queries are run, or to use a result outside the scope
 * of the current transaction, see {@link #list()}.
 *
 * <h2>Important note on semantics</h2>
 *
 * In order to handle very large results, and to minimize memory overhead and maximize
 * performance, results are retrieved lazily. Please see {@link QueryRunner} for
 * important details on the effects of this.
 *
 * The short version is that, if you want a hard guarantee that the underlying query
 * has completed, you need to either call {@link Resource#close()} on the {@link Transaction}
 * or {@link Session} that created this result, or you need to use the result.
 *
 * Calling any method on this interface will guarantee that any write operation has completed on
 * the remote database.
 *
 * @since 1.0
 */
public interface Result extends Iterator<Record>
{
    /**
     * Retrieve the keys of the records this result contains.
     *
     * @return all keys
     */
    List<String> keys();

    /**
     * Test if there is another record we can navigate to in this result.
     * @return true if {@link #next()} will return another record
     */
    @Override boolean hasNext();

    /**
     * Navigate to and retrieve the next {@link Record} in this result.
     *
     * @throws NoSuchRecordException if there is no record left in the stream
     * @return the next record
     */
    @Override Record next();

    /**
     * Return the first record in the result, failing if there is not exactly
     * one record left in the stream
     *
     * Calling this method always exhausts the result, even when {@link NoSuchRecordException} is thrown.
     *
     * @return the first and only record in the stream
     * @throws NoSuchRecordException if there is not exactly one record left in the stream
     */
    Record single() throws NoSuchRecordException;

    /**
     * Investigate the next upcoming record without moving forward in the result.
     *
     * @throws NoSuchRecordException if there is no record left in the stream
     * @return the next record
     */
    Record peek();

    /**
     * Convert this result to a sequential {@link Stream} of records.
     * <p>
     * Result is exhausted when a terminal operation on the returned stream is executed.
     *
     * @return sequential {@link Stream} of records. Empty stream if this result has already been consumed or is empty.
     */
    Stream<Record> stream();

    /**
     * Retrieve and store the entire result stream.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     *
     * Note that this method can only be used if you know that the query that
     * yielded this result returns a finite stream. Some queries can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     *
     * Calling this method exhausts the result.
     *
     * @return list of all remaining immutable records
     */
    List<Record> list();

    /**
     * Retrieve and store a projection of the entire result.
     * This can be used if you want to iterate over the stream multiple times or to store the
     * whole result for later use.
     *
     * Note that this method can only be used if you know that the query that
     * yielded this result returns a finite stream. Some queries can yield
     * infinite results, in which case calling this method will lead to running
     * out of memory.
     *
     * Calling this method exhausts the result.
     *
     * @param mapFunction a function to map from Record to T. See {@link Records} for some predefined functions.
     * @param <T> the type of result list elements
     * @return list of all mapped remaining immutable records
     */
    <T> List<T> list( Function<Record, T> mapFunction );

    /**
     * Return the result summary.
     *
     * If the records in the result is not fully consumed, then calling this method will exhausts the result.
     *
     * If you want to access unconsumed records after summary, you shall use {@link Result#list()} to buffer all records into memory before summary.
     *
     * @return a summary for the whole query result.
     */
    ResultSummary consume();
}
